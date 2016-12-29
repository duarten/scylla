/*
 * Copyright (C) 2016 ScyllaDB
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

#pragma once

#include "clustering_bounds_comparator.hh"
#include "range_tombstone.hh"
#include "streamed_mutation.hh"
#include "tombstone.hh"

namespace db {

namespace view {

class tombstone_tracker final {
    bound_view::compare _cmp;
    tombstone  _partition_tombstone;
    stdx::optional<range_tombstone> _current_range_tombstone;
public:
    explicit tombstone_tracker(const schema& s, tombstone partition_tombstone)
            : _cmp(s)
            , _partition_tombstone(std::move(partition_tombstone)) {
    }

    void apply(range_tombstone&& rt);

    tombstone current_tombstone() const;

    // The rows passed to apply_to() must be in clustering order.
    void apply_to(clustering_row& row);
};

class view_updates final {
    lw_shared_ptr<const db::view::view> _view;
    schema_ptr _base;
    std::unordered_map<partition_key, mutation_partition, partition_key::hashing, partition_key::equality> _updates;
public:
    explicit view_updates(lw_shared_ptr<const db::view::view> view, schema_ptr base)
            : _view(std::move(view))
            , _base(base)
            , _updates(8, partition_key::hashing(*_base), partition_key::equality(*_base)) {
    }

    void move_to(std::vector<mutation>& mutations) &&;

    void generate_update(const partition_key& base_key, const clustering_row& update, const stdx::optional<clustering_row>& existing, gc_clock::time_point now);
private:
    mutation_partition& partition_for(partition_key&& key);
    row_marker compute_row_marker(const clustering_row& base_row) const;
    deletable_row& get_view_row(const partition_key& base_key, const clustering_row& update);
    void create_entry(const partition_key& base_key, const clustering_row& update, gc_clock::time_point now);
    void delete_old_entry(const partition_key& base_key, const clustering_row& existing, gc_clock::time_point now);
    void do_delete_old_entry(const partition_key& base_key, const clustering_row& existing, gc_clock::time_point now);
    void update_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now);
    void replace_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now) {
        create_entry(base_key, update, now);
        delete_old_entry(base_key, existing, now);
    }
};

class view_update_builder final {
    struct impl {
        schema_ptr _schema; // The base schema
        std::vector<view_updates> _view_updates;
        streamed_mutation _updates;
        streamed_mutation _existings;
        tombstone_tracker _update_tombstone_tracker;
        tombstone_tracker _existing_tombstone_tracker;
        mutation_fragment_opt _update;
        mutation_fragment_opt _existing;
        gc_clock::time_point _now;

        impl(schema_ptr s,
            std::vector<view_updates>&& views_to_update,
            streamed_mutation&& updates,
            streamed_mutation&& existings)
                : _schema(std::move(s))
                , _view_updates(std::move(views_to_update))
                , _updates(std::move(updates))
                , _existings(std::move(existings))
                , _update_tombstone_tracker(*_schema, _updates.partition_tombstone())
                , _existing_tombstone_tracker(*_schema, _existings.partition_tombstone())
                , _now(gc_clock::now()) {
        }

        future<std::vector<mutation>> build();

        future<stop_iteration> on_results();

        future<stop_iteration> advance_all();

        future<stop_iteration> advance_updates();

        future<stop_iteration> advance_existings();

        future<stop_iteration> stop() const;

        void generate_update(clustering_row&& update, stdx::optional<clustering_row>&& existing);
    };

    std::unique_ptr<impl> _impl;
public:
    explicit view_update_builder(const schema_ptr& base,
                    std::vector<lw_shared_ptr<view>>&& views_to_update,
                    streamed_mutation&& updates,
                    streamed_mutation&& existings)
            : _impl(std::make_unique<impl>(base,
                        boost::copy_range<std::vector<view_updates>>(views_to_update | boost::adaptors::transformed([&] (auto&& v) {
                            return view_updates(std::move(v), base);
                        })),
                        std::move(updates),
                        std::move(existings))) {
    }

    future<std::vector<mutation>> build() &&;
};

}

}
