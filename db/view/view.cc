/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <boost/range/algorithm/transform.hpp>
#include <boost/range/adaptors.hpp>

#include "clustering_bounds_comparator.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/util.hh"
#include "db/view/view.hh"
#include "db/view/view_update_builder.hh"

namespace db {

namespace view {

shared_ptr<cql3::statements::select_statement> view::select_statement() const {
    if (!_select_statement) {
        std::vector<sstring_view> included;
        if (!_schema->view_info()->include_all_columns()) {
            included.reserve(_schema->all_columns_in_select_order().size());
            boost::transform(_schema->all_columns_in_select_order(), std::back_inserter(included), std::mem_fn(&column_definition::name_as_text));
        }
        auto raw = cql3::util::build_select_statement(_schema->view_info()->base_name(), _schema->view_info()->where_clause(), std::move(included));
        raw->prepare_keyspace(_schema->ks_name());
        raw->set_bound_variables({});
        cql3::cql_stats ignored;
        auto prepared = raw->prepare(service::get_local_storage_proxy().get_db().local(), ignored, true);
        _select_statement = static_pointer_cast<cql3::statements::select_statement>(prepared->statement);
    }
    return _select_statement;
}

const query::partition_slice& view::partition_slice() const {
    if (!_partition_slice) {
        _partition_slice = select_statement()->make_partition_slice(cql3::query_options({ }));
    }
    return *_partition_slice;
}

const dht::partition_range_vector view::partition_ranges() const {
    if (!_partition_ranges) {
        _partition_ranges = select_statement()->get_restrictions()->get_partition_key_ranges(cql3::query_options({ }));
    }
    return *_partition_ranges;
}

bool view::partition_key_matches(const ::schema& base, const dht::decorated_key& key) const {
    dht::ring_position rp(key);
    auto& ranges = partition_ranges();
    return std::any_of(ranges.begin(), ranges.end(), [&] (auto&& range) {
        return range.contains(rp, dht::ring_position_comparator(base));
    });
}

bool view::clustering_prefix_matches(const ::schema& base, const partition_key& key, const clustering_key_prefix& ck) const {
    bound_view::compare less(base);
    auto& ranges = partition_slice().row_ranges(base, key);
    return std::any_of(ranges.begin(), ranges.end(), [&] (auto&& range) {
        auto bounds = bound_view::from_range(range);
        return !less(ck, bounds.first) && !less(bounds.second, ck);
    });
}

bool view::may_be_affected_by(const ::schema& base, const dht::decorated_key& key, const rows_entry& update) const {
    // We can guarantee that the view won't be affected if:
    //  - the primary key is excluded by the view filter (note that this isn't true of the filter on regular columns:
    //    even if an update don't match a view condition on a regular column, that update can still invalidate a
    //    pre-existing entry);
    //  - the update doesn't modify any of the columns impacting the view (where "impacting" the view means that column
    //    is neither included in the view, nor used by the view filter).
    if (!partition_key_matches(base, key) && !clustering_prefix_matches(base, key.key(), update.key())) {
        return false;
    }

    // We want to check if the update modifies any of the columns that are part of the view (in which case the view is
    // affected). But iff the view includes all the base table columns, or the update has either a row deletion or a
    // row marker, we know the view is affected right away.
    if (_schema->view_info()->include_all_columns() || update.row().deleted_at() || update.row().marker().is_live()) {
        return true;
    }

    bool affected = false;
    update.row().cells().for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& cell) {
        affected = _schema->get_column_definition(base.column_at(column_kind::regular_column, id).name());
        return stop_iteration(affected);
    });
    return affected;
}

bool view::matches_view_filter(const ::schema& base, const partition_key& key, const clustering_row& update, gc_clock::time_point now) const {
    return boost::algorithm::all_of(
        select_statement()->get_restrictions()->get_non_pk_restriction() | boost::adaptors::map_values,
        [&] (auto&& r) {
            return r->is_satisfied_by(base, key, update.key(), update.cells(), cql3::query_options({ }), now);
        });
}

void view::set_base_non_pk_column_in_view_pk() {
    for (auto&& base_col : _schema->regular_columns()) {
        auto view_col = _schema->get_column_definition(base_col.name());
        if (view_col && view_col->is_primary_key()) {
            _base_non_pk_column_in_view_pk = view_col;
            return;
        }
    }
    _base_non_pk_column_in_view_pk = nullptr;
}

void tombstone_tracker::apply(range_tombstone&& rt) {
    _current_range_tombstone = std::move(rt);
    _current_range_tombstone->tomb.apply(_partition_tombstone);
}

tombstone tombstone_tracker::current_tombstone() const {
    return _current_range_tombstone ? _current_range_tombstone->tomb : _partition_tombstone;
}

// The rows passed to apply_to() must be in clustering order.
void tombstone_tracker::apply_to(clustering_row& row) {
    if (_cmp(row.key(), _current_range_tombstone->end_bound())) {
        row.apply(_current_range_tombstone->tomb);
    } else {
        _current_range_tombstone = {};
    }
}

void view_updates::move_to(std::vector<mutation>& mutations) && {
    auto& partitioner = dht::global_partitioner();
    std::transform(_updates.begin(), _updates.end(), std::back_inserter(mutations), [&, this] (auto&& m) {
        return mutation(_view->schema(), partitioner.decorate_key(*_base, std::move(m.first)), std::move(m.second));
    });
}

mutation_partition& view_updates::partition_for(partition_key&& key) {
    auto it = _updates.find(key);
    if (it != _updates.end()) {
        return it->second;
    }
    return _updates.emplace(std::move(key), mutation_partition(_view->schema())).first->second;
}

row_marker view_updates::compute_row_marker(const clustering_row& base_row) const {
    /*
     * We need to compute both the timestamp and expiration.
     *
     * For the timestamp, it makes sense to use the bigger timestamp for all view PK columns.
     *
     * This is more complex for the expiration. We want to maintain consistency between the base and the view, so the
     * entry should only exist as long as the base row exists _and_ has non-null values for all the columns that are part
     * of the view PK.
     * Which means we really have 2 cases:
     *   1) There is a column that is not in the base PK but is in the view PK. In that case, as long as that column
     *      lives, the view entry does too, but as soon as it expires (or is deleted for that matter) the entry also
     *      should expire. So the expiration for the view is the one of that column, regardless of any other expiration.
     *      To take an example of that case, if you have:
     *        CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a, b))
     *        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)
     *        INSERT INTO t(a, b) VALUES (0, 0) USING TTL 3;
     *        UPDATE t SET c = 0 WHERE a = 0 AND b = 0;
     *      then even after 3 seconds elapsed, the row will still exist (it just won't have a "row marker" anymore) and so
     *      the MV should still have a corresponding entry.
     *   2) The columns for the base and view PKs are exactly the same. In that case, the view entry should live
     *      as long as the base row lives. This means the view entry should only expire once *everything* in the
     *      base row has expired. So, the row TTL should be the max of any other TTL. This is particularly important
     *      in the case where the base row has a TTL, but a column *absent* from the view holds a greater TTL.
     */

    auto marker = base_row.marker();
    auto* col = _view->base_non_pk_column_in_view_pk();
    if (col) {
        // Note: multi-cell columns can't be part of the primary key.
        auto cell = base_row.cells().cell_at(col->id).as_atomic_cell();
        auto timestamp = std::max(marker.timestamp(), cell.timestamp());
        return cell.is_live_and_has_ttl() ? row_marker(timestamp, cell.ttl(), cell.expiry()) : row_marker(timestamp);
    }

    if (!marker.is_expiring()) {
        return marker;
    }

    auto ttl = marker.ttl();
    auto expiry = marker.expiry();
    auto maybe_update_expiry_and_ttl = [&] (atomic_cell_view&& cell) {
        // Note: Cassandra compares cell.ttl() here, but that seems very wrong.
        // See CASSANDRA-13127.
        if (cell.is_live_and_has_ttl() && cell.expiry() > expiry) {
            expiry = cell.expiry();
            ttl = cell.ttl();
        }
    };

    base_row.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        auto& def = _base->regular_column_at(id);
        if (def.is_atomic()) {
            maybe_update_expiry_and_ttl(c.as_atomic_cell());
        } else {
            static_pointer_cast<const collection_type_impl>(def.type)->for_each_cell(c.as_collection_mutation(), maybe_update_expiry_and_ttl);
        }
    });

    return row_marker(marker.timestamp(), ttl, expiry);
}

deletable_row& view_updates::get_view_row(const partition_key& base_key, const clustering_row& update) {
    auto get_value = boost::adaptors::transformed([&, this] (const column_definition& cdef) {
        auto* base_col = _base->get_column_definition(cdef.name());
        assert(base_col);
        switch (base_col->kind) {
        case column_kind::partition_key:
            return base_key.get_component(*_base, base_col->position());
        case column_kind::clustering_key:
            return update.key().get_component(*_base, base_col->position());
        default:
            auto& c = update.cells().cell_at(base_col->id);
            if (base_col->is_atomic()) {
                return c.as_atomic_cell().value();
            }
            return c.as_collection_mutation().data;
        }
    });
    auto& view_schema = *_view->schema();
    auto& partition = partition_for(partition_key::from_range(view_schema.partition_key_columns() | get_value));
    auto ckey = clustering_key::from_range(view_schema.clustering_key_columns() | get_value);
    return partition.clustered_row(view_schema, std::move(ckey));
}

static const column_definition* view_column(const schema& base, const schema& view, column_id base_id) {
    return view.get_column_definition(base.regular_column_at(base_id).name());
}

static void add_cells_to_view(const schema& base, const schema& view, const row& base_cells, row& view_cells) {
    base_cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        auto* view_col = view_column(base, view, id);
        if (view_col && !view_col->is_primary_key()) {
            view_cells.append_cell(view_col->id, c);
        }
    });
}

/**
 * Creates a view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before applying anything.
 */
void view_updates::create_entry(const partition_key& base_key, const clustering_row& update, gc_clock::time_point now) {
    if (!_view->matches_view_filter(*_base, base_key, update, now)) {
        return;
    }
    deletable_row& r = get_view_row(base_key, update);
    r.apply(compute_row_marker(update));
    r.apply(update.tomb());
    add_cells_to_view(*_base, *_view->schema(), update.cells(), r.cells());
}

/**
 * Deletes the view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before bothering.
 */
void view_updates::delete_old_entry(const partition_key& base_key, const clustering_row& existing, gc_clock::time_point now) {
    // Before deleting an old entry, make sure it was matching the view filter
    // (otherwise there is nothing to delete)
    if (_view->matches_view_filter(*_base, base_key, existing, now)) {
        do_delete_old_entry(base_key, existing, now);
    }
}

void view_updates::do_delete_old_entry(const partition_key& base_key, const clustering_row& existing, gc_clock::time_point now) {
    // We delete the old row using a shadowable row tombstone, making sure that
    // the tombstone deletes everything in the row (or it might still show up).
    // FIXME: If the entry is "resurected" by a later update, we would need to
    // ensure that the timestamp for the entry then is bigger than the tombstone
    // we're just inserting, which is not currently guaranteed. See CASSANDRA-11500
    // for details.
    auto& view_schema = *_view->schema();
    auto ts = existing.marker().timestamp();
    auto set_max_ts = [&ts] (atomic_cell_view&& cell) {
        ts = std::max(ts, cell.timestamp());
    };
    existing.cells().for_each_cell([&, this] (column_id id, const atomic_cell_or_collection& cell) {
        auto* def = view_column(*_base, view_schema, id);
        if (!def) {
            return;
        }
        if (def->is_atomic()) {
            set_max_ts(cell.as_atomic_cell());
        } else {
            static_pointer_cast<const collection_type_impl>(def->type)->for_each_cell(cell.as_collection_mutation(), set_max_ts);
        }
    });
    get_view_row(base_key, existing).apply(tombstone(ts, now));
}

/**
 * Creates the updates to apply to the existing view entry given the base table row before
 * and after the update, assuming that the update hasn't changed to which view entry the
 * row corresponds (that is, we know the columns composing the view PK haven't changed).
 *
 * This method checks that the base row (before and after) matches the view filter before
 * applying anything.
 */
void view_updates::update_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now) {
    // While we know update and existing correspond to the same view entry,
    // they may not match the view filter.
    if (!_view->matches_view_filter(*_base, base_key, existing, now)) {
        create_entry(base_key, update, now);
        return;
    }
    if (!_view->matches_view_filter(*_base, base_key, update, now)) {
        do_delete_old_entry(base_key, existing, now);
        return;
    }

    deletable_row& r = get_view_row(base_key, update);
    r.apply(compute_row_marker(update));
    r.apply(update.tomb());

    auto diff = update.cells().difference(*_base, column_kind::regular_column, existing.cells());
    add_cells_to_view(*_base, *_view->schema(), diff, r.cells());
}

void view_updates::generate_update(
        const partition_key& base_key,
        const clustering_row& update,
        const stdx::optional<clustering_row>& existing,
        gc_clock::time_point now) {
    // Note that none of the base PK columns will differ since we're intrinsically dealing
    // with the same base row. So we have to check 3 things:
    //   1) that the clustering key doesn't have a null, which can happen for compact tables. If that's the case,
    //      there is no corresponding entries.
    //   2) if there is a column not part of the base PK in the view PK, whether it is changed by the update.
    //   3) whether the update actually matches the view SELECT filter

    if (!update.key().is_full(*_base)) {
        return;
    }

    auto* col = _view->base_non_pk_column_in_view_pk();
    if (!col) {
        // The view entry is necessarily the same pre and post update.
        if (existing && !existing->empty()) {
            if (update.empty()) {
                delete_old_entry(base_key, *existing, now);
            } else {
                update_entry(base_key, update, *existing, now);
            }
        }  else if (!update.empty()) {
            create_entry(base_key, update, now);
        }
        return;
    }

    auto col_id = col->id;
    auto* after = update.cells().find_cell(col_id);
    if (existing) {
        auto* before = existing->cells().find_cell(col_id);
        if (before) {
            if (after) {
                auto cmp = compare_atomic_cell_for_merge(before->as_atomic_cell(), after->as_atomic_cell());
                if (cmp == 0) {
                    replace_entry(base_key, update, *existing, now);
                } else {
                    update_entry(base_key, update, *existing, now);
                }
            } else {
                delete_old_entry(base_key, *existing, now);
            }
            return;
        }
    }

    // No existing row or the cell wasn't live
    if (after) {
        create_entry(base_key, update, now);
    }
}

future<std::vector<mutation>> view_update_builder::impl::build() {
    return advance_all().then([this] (auto&& ignored) {
        return repeat([this] {
            return this->on_results();
        });
    }).then([this] {
        std::vector<mutation> mutations;
        for (auto&& update : _view_updates) {
            std::move(update).move_to(mutations);
        }
        return mutations;
    });
}

void view_update_builder::impl::generate_update(clustering_row&& update, stdx::optional<clustering_row>&& existing) {
    // If we have no update at all, we shouldn't get there.
    if (update.empty()) {
        throw std::logic_error("Empty materialized view updated");
    }

    auto gc_before = _now - _schema->gc_grace_seconds();

    // We allow existing to be disengaged, which we treat the same as an empty row.
    if (existing) {
        existing->marker().compact_and_expire(tombstone(), _now, always_gc, gc_before);
        existing->cells().compact_and_expire(*_schema, column_kind::regular_column, tombstone(), _now, always_gc, gc_before);
        update.apply(*_schema, *existing);
    }

    update.marker().compact_and_expire(tombstone(), _now, always_gc, gc_before);
    update.cells().compact_and_expire(*_schema, column_kind::regular_column, tombstone(), _now, always_gc, gc_before);

    for (auto&& v : _view_updates) {
        v.generate_update(_updates.key(), update, existing, _now);
    }
}

future<stop_iteration> view_update_builder::impl::on_results() {
    if (_update && _existing) {
        int cmp = position_in_partition::tri_compare(*_schema)(_update->position(), _existing->position());
        if (cmp < 0) {
            // We have an update where there was nothing before
            if (_update->is_range_tombstone()) {
                _update_tombstone_tracker.apply(std::move(_update->as_range_tombstone()));
            } else {
                auto& update = _update->as_clustering_row();
                _update_tombstone_tracker.apply_to(update);
                auto tombstone = _existing_tombstone_tracker.current_tombstone();
                auto existing = tombstone
                              ? stdx::optional<clustering_row>(stdx::in_place, update.key(), std::move(tombstone), row_marker(), ::row())
                              : stdx::nullopt;
                generate_update(std::move(update), std::move(existing));
            }
            return advance_updates();
        }
        if (cmp < 0) {
            // We have something existing but no update (which will happen either because it's a range tombstone marker in
            // existing, or because we've fetched the existing row due to some partition/range deletion in the updates)
            if (_existing->is_range_tombstone()) {
                _existing_tombstone_tracker.apply(std::move(_existing->as_range_tombstone()));
            } else {
                auto& existing = _existing->as_clustering_row();
                _existing_tombstone_tracker.apply_to(existing);
                auto tombstone = _update_tombstone_tracker.current_tombstone();
                // The way we build the read command used for existing rows, we should always have a non-empty
                // tombstone, since we wouldn't have read the existing row otherwise. We don't assert that in case the
                // read method ever changes.
                if (tombstone) {
                    auto update = clustering_row(existing.key(), std::move(tombstone), row_marker(), ::row());
                    generate_update(std::move(update), { std::move(existing) });
                }
            }
            return advance_existings();
        }
        // We're updating a row that had pre-existing data
        if (_update->is_range_tombstone()) {
            assert(_existing->is_range_tombstone());
            _existing_tombstone_tracker.apply(std::move(_existing->as_range_tombstone()));
            _update_tombstone_tracker.apply(std::move(_update->as_range_tombstone()));
        } else {
            assert(!_existing->is_range_tombstone());
            _update_tombstone_tracker.apply_to(_update->as_clustering_row());
            _existing_tombstone_tracker.apply_to(_existing->as_clustering_row());
            generate_update(std::move(_update->as_clustering_row()), { std::move(_existing->as_clustering_row()) });
        }
        return advance_all();
    }

    auto tombstone = _update_tombstone_tracker.current_tombstone();
    if (tombstone && _existing) {
        // We don't care if it's a range tombstone, as we're only looking for existing entries that get deleted
        if (!_existing->is_range_tombstone()) {
            auto& existing = _existing->as_clustering_row();
            auto update = clustering_row(existing.key(), std::move(tombstone), row_marker(), ::row());
            generate_update(std::move(update), { std::move(existing) });
        }
        return advance_existings();
    }

    // If we have updates and it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it
    if (_update && !_update->is_range_tombstone()) {
        generate_update(std::move(_update->as_clustering_row()), { });
        return advance_updates();
    }

    return stop();
}

future<stop_iteration> view_update_builder::impl::advance_all() {
    return when_all(_updates(), _existings()).then([this] (auto&& fragments) mutable {
        _update = std::move(std::get<mutation_fragment_opt>(std::get<0>(fragments).get()));
        _existing = std::move(std::get<mutation_fragment_opt>(std::get<1>(fragments).get()));
        return stop_iteration::no;
    });
}

future<stop_iteration> view_update_builder::impl::advance_updates() {
    return _updates().then([this] (auto&& update) mutable {
        _update = std::move(update);
        return stop_iteration::no;
    });
}

future<stop_iteration> view_update_builder::impl::advance_existings() {
    return _existings().then([this] (auto&& existing) mutable {
        _existing = std::move(existing);
        return stop_iteration::no;
    });
}

future<stop_iteration> view_update_builder::impl::stop() const {
    return make_ready_future<stop_iteration>(stop_iteration::yes);
}

future<std::vector<mutation>> view_update_builder::build() && {
    auto f = _impl->build();
    return f.finally([impl = std::move(_impl)] { });
}

} // namespace view
} // namespace db

