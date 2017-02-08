/*
 * Copyright 2015 ScyllaDB
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

#include <unordered_map>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/shared_future.hh>
#include "schema.hh"
#include "frozen_schema.hh"

class schema_registry;

using async_schema_loader = std::function<future<frozen_schema_and_views>(table_schema_version)>;
using schema_loader = std::function<frozen_schema(table_schema_version)>;

class schema_version_not_found : public std::runtime_error {
public:
    schema_version_not_found(table_schema_version v);
};

class schema_version_loading_failed : public std::runtime_error {
public:
    schema_version_loading_failed(table_schema_version v);
};

struct schema_and_views {
    schema_ptr schema;
    std::vector<view_ptr> views;
};

//
// Presence in schema_registry is controlled by different processes depending on
// life cycle stage:
//   1) Initially it's controlled by the loader. When loading fails, entry is removed by the loader.
//   2) When loading succeeds, the entry is controlled by live schema_ptr. It remains present as long as
//      there's any live schema_ptr.
//   3) When last schema_ptr dies, entry is deactivated. Currently it is removed immediately, later we may
//      want to keep it around for some time to reduce cache misses.
//
// In addition to the above the entry is controlled by lw_shared_ptr<> to cope with races between loaders.
//
class schema_registry_entry : public enable_lw_shared_from_this<schema_registry_entry> {
    enum class state {
        INITIAL, LOADING, LOADED
    };

    state _state;
    table_schema_version _version; // always valid
    schema_registry& _registry; // always valid

    async_schema_loader _loader; // valid when state == LOADING
    promise<schema_ptr> _schema_promise; // valid when state == LOADING
    shared_future<schema_ptr> _schema_future; // valid when state == LOADING

    std::experimental::optional<frozen_schema> _frozen_schema; // engaged when state == LOADED
    // valid when state == LOADED
    // This is != nullptr when there is an alive schema_ptr associated with this entry.
    const ::schema* _schema = nullptr;

    enum class sync_state { NOT_SYNCED, SYNCING, SYNCED };
    sync_state _sync_state;
    promise<> _synced_promise; // valid when _sync_state == SYNCING
    shared_future<> _synced_future; // valid when _sync_state == SYNCING

    std::unordered_map<utils::UUID, table_schema_version> _views;

    friend class schema_registry;
public:
    schema_registry_entry(table_schema_version v, schema_registry& r);
    schema_registry_entry(schema_registry_entry&&) = delete;
    schema_registry_entry(const schema_registry_entry&) = delete;
    ~schema_registry_entry();
    schema_ptr load(frozen_schema&&, std::vector<frozen_schema>&& frozen_views);
    future<schema_and_views> start_loading(async_schema_loader);
    schema_ptr get_schema(); // call only when state >= LOADED
    // Can be called from other shards
    bool is_synced() const;
    // Initiates asynchronous schema sync or returns ready future when is already synced.
    future<> maybe_sync(std::function<future<>()> sync);
    // Marks this schema version as synced. Syncing cannot be in progress.
    void mark_synced();
    // Can be called from other shards
    frozen_schema frozen() const;
    // Can be called from other shards
    table_schema_version version() const { return _version; }
public:
    // Called by class schema
    void detach_schema() noexcept;
private:
    void register_view(const view_ptr&);
};

//
// Keeps track of different versions of table schemas. A per-shard object.
//
// For every schema_ptr obtained through getters, as long as the schema pointed to is
// alive the registry will keep its entry. To ensure remote nodes can query current node
// for schema version, make sure that schema_ptr for the request is alive around the call.
//
class schema_registry {
    std::unordered_map<table_schema_version, lw_shared_ptr<schema_registry_entry>> _entries;
    std::unordered_map<utils::UUID, table_schema_version> _latest;
    friend class schema_registry_entry;
    schema_registry_entry& get_entry(table_schema_version) const;
    void add_entry(const schema_ptr&, lw_shared_ptr<schema_registry_entry>, const std::vector<view_ptr>&);
public:
    // Looks up schema by version or loads it using supplied loader.
    schema_ptr get_or_load(table_schema_version, const schema_loader&);

    // Looks up schema by version or returns an empty pointer if not available.
    schema_ptr get_or_null(table_schema_version) const;

    // Like get_or_load() which takes schema_loader but the loader may be
    // deferring. The loader is copied must be alive only until this method
    // returns. If the loader fails, the future resolves with
    // schema_version_loading_failed.
    future<schema_and_views> get_or_load(table_schema_version, const async_schema_loader&);

    // Looks up schema version. Throws schema_version_not_found when not found
    // or loading is in progress.
    schema_ptr get(table_schema_version) const;

    // Looks up schema version. Throws schema_version_not_found when not found
    // or loading is in progress.
    frozen_schema_and_views get_frozen(table_schema_version) const;

    // Attempts to add given schema to the registry. If the registry already
    // knows about the schema, returns existing entry, otherwise returns back
    // the schema which was passed as argument. Users should prefer to use the
    // schema_ptr returned by this method instead of the one passed to it,
    // because doing so ensures that the entry will be kept in the registry as
    // long as the schema is actively used. Associates already learned view
    // schemas with the specified one, so they can be loaded and sent to a
    // remote node as a single unit, for write operations.
    schema_ptr learn(const schema_ptr&, const std::vector<view_ptr>&);
};

schema_registry& local_schema_registry();

// Schema pointer which can be safely accessed/passed across shards via
// const&. Useful for ensuring that schema version obtained on one shard is
// automatically propagated to other shards, no matter how long the processing
// chain will last.
class global_schema_ptr {
    schema_ptr _ptr;
    unsigned _cpu_of_origin;
public:
    // Note: the schema_ptr must come from the current shard and can't be nullptr.
    global_schema_ptr(const schema_ptr&);
    // The other may come from a different shard.
    global_schema_ptr(const global_schema_ptr& other);
    // The other must come from current shard.
    global_schema_ptr(global_schema_ptr&& other);
    // May be invoked across shards. Always returns an engaged pointer.
    schema_ptr get() const;
    operator schema_ptr() const { return get(); }
};
