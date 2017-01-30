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

namespace db {

namespace view {

cql3::statements::select_statement& view::select_statement() const {
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
    return *_select_statement;
}

const query::partition_slice& view::partition_slice() const {
    if (!_partition_slice) {
        _partition_slice = select_statement().make_partition_slice(cql3::query_options({ }));
    }
    return *_partition_slice;
}

const dht::partition_range_vector& view::partition_ranges() const {
    if (!_partition_ranges) {
        _partition_ranges = select_statement().get_restrictions()->get_partition_key_ranges(cql3::query_options({ }));
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
    return clustering_prefix_matches(base, key, update.key()) &&
                boost::algorithm::all_of(
                    select_statement().get_restrictions()->get_non_pk_restriction() | boost::adaptors::map_values,
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

class tombstone_tracker final {
    bound_view::compare _cmp;
    tombstone  _partition_tombstone;
    stdx::optional<range_tombstone> _current_range_tombstone;
public:
    explicit tombstone_tracker(const schema& s, tombstone partition_tombstone)
            : _cmp(s)
            , _partition_tombstone(std::move(partition_tombstone)) {
    }

    tombstone current_tombstone() const {
        return _current_range_tombstone ? _current_range_tombstone->tomb : _partition_tombstone;
    }

    void apply(range_tombstone&& rt) {
        _current_range_tombstone = std::move(rt);
        _current_range_tombstone->tomb.apply(_partition_tombstone);
    }

    // The rows passed to apply_to() must be in clustering order.
    void apply_to(clustering_row& row) {
        if (_cmp(row.key(), _current_range_tombstone->end_bound())) {
            row.apply(_current_range_tombstone->tomb);
        } else {
            _current_range_tombstone = {};
        }
    }
};

class view_updates final {
    lw_shared_ptr<const db::view::view> _view;
    schema_ptr _base;
    std::unordered_map<partition_key, mutation_partition, partition_key::hashing, partition_key::equality> _updates;
public:
    explicit view_updates(lw_shared_ptr<const db::view::view> view, schema_ptr base)
            : _view(std::move(view))
            , _base(std::move(base))
            , _updates(8, partition_key::hashing(*_base), partition_key::equality(*_base)) {
    }

    void move_to(std::vector<mutation>& mutations) && {
        auto& partitioner = dht::global_partitioner();
        std::transform(_updates.begin(), _updates.end(), std::back_inserter(mutations), [&, this] (auto&& m) {
            return mutation(_view->schema(), partitioner.decorate_key(*_base, std::move(m.first)), std::move(m.second));
        });
    }

private:
    mutation_partition& partition_for(partition_key&& key) {
        auto it = _updates.find(key);
        if (it != _updates.end()) {
            return it->second;
        }
        return _updates.emplace(std::move(key), mutation_partition(_view->schema())).first->second;
    }
    row_marker compute_row_marker(const clustering_row& base_row) const;
};

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

} // namespace view
} // namespace db

