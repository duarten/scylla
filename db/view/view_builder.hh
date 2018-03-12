/*
 * Copyright (C) 2018 ScyllaDB
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

#include "database_fwd.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "dht/i_partitioner.hh"
#include "keys.hh"
#include "query-request.hh"
#include "service/migration_listener.hh"
#include "service/migration_manager.hh"
#include "sstables/sstable_set.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/serialized_action.hh"
#include "utils/UUID.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <optional>
#include <unordered_map>
#include <vector>

namespace db::view {

/**
 * The view_builder is a sharded service responsible for building all defined materialized views.
 * This process entails walking over the existing data in a given base table, and using it to
 * calculate and insert the respective entries for one or more views.
 *
 * We employ a flat_mutation_reader for each base table for which we're building views.
 *
 * View building is necessarily a sharded process. That means that on restart, if the number of shards
 * has changed, we need to calculate the most conservative token range that has been built, and build
 * the remainder.
 *
 * Interaction with the system tables:
 *   - When we start building a view, we add an entry to the scylla_views_builds_in_progress
 *     system table. If the node restarts at this point, we'll consider these newly inserted
 *     views as having made no progress, and we'll treat them as new views;
 *   - When we finish a build step, we update the progress of the views that we built during
 *     this step by writing the next token to the scylla_views_builds_in_progress table. If
 *     the node restarts here, we'll start building the views at the token in the next_token column.
 *   - When we finish building a view, we mark it as completed in the built views system table, and
 *     remove it from the in-progress system table. Under failure, the following can happen:
 *          * When we fail to mark the view as built, we'll redo the last step upon node reboot;
 *          * When we fail to delete the in-progress record, upon reboot we'll remove this record.
 *     A view is marked as completed only when all shards have finished their share of the work, that is,
 *     if a view is not built, then all shards will still have an entry in the in-progress system table,
 *   - A view that a shard finished building, but not all other shards, remains in the in-progress system
 *     table, with first_token == next_token.
 * Interaction with the distributed system table (view_build_status):
 *   - When we start building a view, we mark the view build as being in-progress;
 *   - When we finish building a view, we mark the view as being built. Upon failure,
 *     we ensure that if the view is in the in-progress system table, then it may not
 *     have been written to this table. We don't load the built views from this table
 *     when starting. When starting, the following happens:
 *          * If the view is in the system.built_views table and not the in-progress
 *            system table, then it will be in view_build_status;
 *          * If the view is in the system.built_views table and not in this one, it
 *            will still be in the in-progress system table - we detect this and mark
 *            it as built in this table too, keeping the invariant;
 *          * If the view is in this table but not in system.built_views, then it will
 *            also be in the in-progress system table - we don't detect this and will
 *            redo the missing step, for simplicity.
 */
class view_builder final : public service::migration_listener::only_view_notifications {
    /**
     * Keeps track of the build progress for a particular view.
     * When the view is built, next_token == first_token.
     */
    struct view_build_status final {
        view_ptr view;
        dht::token first_token;
        std::optional<dht::token> next_token;
    };

    /**
     * Keeps track of the build progress for all the views of a particular
     * base table. Each execution of the build step comprises a query of
     * the base table for the selected range.
     *
     * We pin the set of sstables that potentially contain data that should be added to a
     * view (they are pinned by the flat_mutation_reader). Adding a view v' overwrites the
     * set of pinned sstables, regardless of there being another view v'' being built. The
     * new set will potentially contain new data already in v'', written as part of the write
     * path. We assume this case is rare and optimize for fewer disk space in detriment of
     * network bandwidth.
     */
    struct build_step final {
        // Ensure we pin the column_family. It may happen that all views are removed,
        // and that the base table is too before we can detect it.
        lw_shared_ptr<column_family> base;
        query::partition_slice pslice;
        dht::partition_range prange;
        flat_mutation_reader reader{nullptr};
        dht::decorated_key current_key{dht::minimum_token(), partition_key::make_empty()};
        std::vector<view_build_status> build_status;

        const dht::token& current_token() const {
            return current_key.token();
        }
    };

    using base_to_build_step_type = std::unordered_map<utils::UUID, build_step>;

    database& _db;
    db::system_distributed_keyspace& _sys_dist_ks;
    service::migration_manager& _mm;
    base_to_build_step_type _base_to_build_step;
    base_to_build_step_type::iterator _current_step = _base_to_build_step.end();
    serialized_action _build_step{std::bind(&view_builder::do_build_step, this)};
    // Ensures bookkeeping operations are serialized, meaning that while we execute
    // a build step we don't consider newly added or removed views. This simplifies
    // the algorithms. Also synchronizes an operation wrt. a call to stop().
    seastar::semaphore _sem{1};
    seastar::abort_source _as;

public:
    view_builder(database&, db::system_distributed_keyspace&, service::migration_manager&);
    view_builder(view_builder&&) = delete;

    /**
     * Loads the state stored in the system tables to resume building the existing views.
     * Requires that all views have been loaded from the system tables and are accessible
     * through the database, and that the commitlog has been replayed.
     */
    future<> start();

    /**
     * Stops the view building process.
     */
    future<> stop();

    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override;
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override;
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override;

private:
    build_step& get_or_create_build_step(utils::UUID);
    void initialize_reader_at_current_token(build_step&);
    void load_view_status(view_build_status, std::unordered_set<utils::UUID>&);
    void reshard(std::vector<std::vector<view_build_status>>, std::unordered_set<utils::UUID>&);
    future<> calculate_shard_build_step(std::vector<system_keyspace::view_name>, std::vector<system_keyspace::view_build_progress>);
    future<> add_new_view(view_ptr, build_step&);
    future<> do_build_step();
};

}