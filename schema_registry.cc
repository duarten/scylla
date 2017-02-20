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

#include <seastar/core/sharded.hh>

#include <boost/range/adaptor/transformed.hpp>

#include "schema_registry.hh"
#include "canonical_mutation.hh"
#include "log.hh"
#include "schema_mutations.hh"
#include "utils/UUID.hh"
#include "idl/uuid.dist.hh"
#include "idl/frozen_schema.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_schema.dist.impl.hh"

static logging::logger logger("schema_registry");

static thread_local schema_registry registry;

schema_version_not_found::schema_version_not_found(table_schema_version v)
        : std::runtime_error{sprint("Schema version %s not found", v)}
{ }

schema_version_loading_failed::schema_version_loading_failed(table_schema_version v)
        : std::runtime_error{sprint("Failed to load schema version %s", v)}
{ }

schema_registry_entry::~schema_registry_entry() {
    if (_schema) {
        _schema->_registry_entry = nullptr;
    }
}

schema_registry_entry::schema_registry_entry(table_schema_version v, schema_registry& r)
    : _state(state::INITIAL)
    , _version(v)
    , _registry(r)
    , _sync_state(sync_state::NOT_SYNCED)
    , _view_state(view_state::UNMATCHED)
{ }

schema_ptr schema_registry::learn(const schema_ptr& s) {
    if (s->registry_entry()) {
        return std::move(s);
    }
    auto i = _entries.find(s->version());
    if (i != _entries.end()) {
        return i->second->get_schema();
    }
    logger.debug("Learning about version {} of {}.{}", s->version(), s->ks_name(), s->cf_name());
    auto e_ptr = make_lw_shared<schema_registry_entry>(s->version(), *this);
    auto loaded_s = e_ptr->load(frozen_schema(s));
    _entries.emplace(s->version(), e_ptr);
    return loaded_s;
}

void schema_registry::learn_views(const schema_ptr& base, const std::vector<view_ptr>& views) {
    get_entry(base->version()).set_views(views);
}

void schema_registry::unlearn_view(const schema_ptr& base, const view_ptr& view) {
    get_entry(base->version()).unset_view(get_entry(view->version()));
}

schema_registry_entry& schema_registry::get_entry(table_schema_version v) const {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        throw schema_version_not_found(v);
    }
    schema_registry_entry& e = *i->second;
    if (e._state != schema_registry_entry::state::LOADED) {
        throw schema_version_not_found(v);
    }
    return e;
}

schema_ptr schema_registry::get(table_schema_version v) const {
    return get_entry(v).get_schema();
}

frozen_schema schema_registry::get_frozen(table_schema_version v) const {
    return get_entry(v).frozen();
}

future<frozen_schema_and_views> schema_registry::get_frozen_with_views_eventually(table_schema_version v) const {
    return get_entry(v).get_frozen_with_views_eventually();
}

future<schema_and_views> schema_registry::get_or_load(table_schema_version v, const async_schema_loader& loader) {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        auto e_ptr = make_lw_shared<schema_registry_entry>(v, *this);
        auto f = e_ptr->start_loading(loader);
        _entries.emplace(v, e_ptr);
        return f;
    }
    schema_registry_entry& e = *i->second;
    if (e._state == schema_registry_entry::state::LOADING) {
        return e._schema_promise.get_shared_future();
    }
    return e.get_with_views_eventually();
}

schema_ptr schema_registry::get_or_null(table_schema_version v) const {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        return nullptr;
    }
    schema_registry_entry& e = *i->second;
    if (e._state != schema_registry_entry::state::LOADED) {
        return nullptr;
    }
    return e.get_schema();
}

schema_ptr schema_registry::get_or_load(table_schema_version v, const schema_loader& loader) {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        auto e_ptr = make_lw_shared<schema_registry_entry>(v, *this);
        auto s = e_ptr->load(loader(v));
        _entries.emplace(v, e_ptr);
        return s;
    }
    schema_registry_entry& e = *i->second;
    if (e._state == schema_registry_entry::state::LOADING) {
        return e.load(loader(v));
    }
    return e.get_schema();
}

schema_ptr schema_registry_entry::load(frozen_schema fs) {
    _frozen_schema = std::move(fs);
    auto s = get_schema();
    if (_state == state::LOADING) {
        _schema_promise.set_value(schema_and_views{s, {}});
        _schema_promise = {};
    }
    _state = state::LOADED;
    logger.trace("Loaded {} = {}", _version, *s);
    return s;
}

void schema_registry_entry::load(frozen_schema_and_views fs) {
    std::vector<view_ptr> views;
    views.reserve(fs.views().size());
    for (auto&& fv : std::move(fs).views()) {
        auto in = ser::as_input_stream(fv.representation());
        auto sv = ser::deserialize(in, boost::type<ser::schema_view>());
        auto version = sv.version();
        auto s = _registry.get_or_load(version, [&fv] (table_schema_version) {
            return std::move(fv);
        });
        views.push_back(view_ptr(std::move(s)));
    }
    set_views(views);
    _frozen_schema = std::move(fs).schema();
    auto sav = schema_and_views{get_schema(), std::move(views)};
    logger.trace("Loaded {} = {} (with views)", _version, *sav.schema);
    if (_state == state::LOADING) {
        _schema_promise.set_value(std::move(sav));
        _schema_promise = {};
    }
    _state = state::LOADED;
}

future<schema_and_views> schema_registry_entry::start_loading(async_schema_loader loader) {
    _loader = std::move(loader);
    auto f = _loader(_version);
    auto sf = _schema_promise.get_shared_future();
    _state = state::LOADING;
    logger.trace("Loading {}", _version);
    f.then_wrapped([self = shared_from_this(), this] (future<frozen_schema_and_views>&& f) {
        _loader = {};
        if (_state != state::LOADING) {
            logger.trace("Loading of {} aborted", _version);
            return;
        }
        try {
            try {
                load(f.get0());
            } catch (...) {
                std::throw_with_nested(schema_version_loading_failed(_version));
            }
        } catch (...) {
            logger.debug("Loading of {} failed: {}", _version, std::current_exception());
            _schema_promise.set_exception(std::current_exception());
            _registry._entries.erase(_version);
        }
    });
    return sf;
}

schema_ptr schema_registry_entry::get_schema() {
    if (!_schema) {
        logger.trace("Activating {}", _version);
        auto s = _frozen_schema->unfreeze();
        if (s->version() != _version) {
            throw std::runtime_error(sprint("Unfrozen schema version doesn't match entry version (%s): %s", _version, *s));
        }
        s->_registry_entry = this;
        _schema = &*s;
        return s;
    } else {
        return _schema->shared_from_this();
    }
}

void schema_registry_entry::detach_schema() noexcept {
    logger.trace("Deactivating {}", _version);
    _schema = nullptr;
    // TODO: keep the entry for a while (timer)
    try {
        _registry._entries.erase(_version);
    } catch (...) {
        logger.error("Failed to erase schema version {}: {}", _version, std::current_exception());
    }
}

frozen_schema schema_registry_entry::frozen() const {
    assert(_state >= state::LOADED);
    return *_frozen_schema;
}

future<> schema_registry_entry::maybe_sync(std::function<future<>()> syncer) {
    switch (_sync_state) {
        case schema_registry_entry::sync_state::SYNCED:
            return make_ready_future<>();
        case schema_registry_entry::sync_state::SYNCING:
            return _synced_promise.get_shared_future();
        case schema_registry_entry::sync_state::NOT_SYNCED: {
            logger.debug("Syncing {}", _version);
            _synced_promise = {};
            auto f = do_with(std::move(syncer), [] (auto& syncer) {
                return syncer();
            });
            auto sf = _synced_promise.get_shared_future();
            _sync_state = schema_registry_entry::sync_state::SYNCING;
            f.then_wrapped([this, self = shared_from_this()] (auto&& f) {
                if (_sync_state != sync_state::SYNCING) {
                    return;
                }
                if (f.failed()) {
                    logger.debug("Syncing of {} failed", _version);
                    _sync_state = schema_registry_entry::sync_state::NOT_SYNCED;
                    _synced_promise.set_exception(f.get_exception());
                } else {
                    logger.debug("Synced {}", _version);
                    _sync_state = schema_registry_entry::sync_state::SYNCED;
                    _synced_promise.set_value();
                }
            });
            return sf;
        }
        default:
            assert(0);
    }
}

bool schema_registry_entry::is_synced() const {
    return _sync_state == sync_state::SYNCED;
}

void schema_registry_entry::mark_synced() {
    if (_sync_state == sync_state::SYNCING) {
        _synced_promise.set_value();
    }
    _sync_state = sync_state::SYNCED;
    logger.debug("Marked {} as synced", _version);
}

void schema_registry_entry::set_views(const std::vector<view_ptr>& views) {
    if (_view_state == view_state::MATCHING) {
        _views_matched_promise.set_value();
        _views_matched_promise = {};
    }
    _view_state = view_state::MATCHED;
    _views = boost::copy_range<std::vector<lw_shared_ptr<schema_registry_entry>>>(views | boost::adaptors::transformed([this] (auto&& v) {
        return _registry.get_entry(v->version()).shared_from_this();
    }));
    if (!_views.empty()) {
        logger.debug("Matched views for base table {}", _version);
    }
}

void schema_registry_entry::unset_view(const schema_registry_entry& v) {
    _views.erase(std::remove_if(_views.begin(), _views.end(), [&v] (auto&& view) {
        return view.get() == &v;
    }), _views.end());
}

future<frozen_schema_and_views> schema_registry_entry::get_frozen_with_views_eventually() {
    switch (_view_state) {
    case schema_registry_entry::view_state::MATCHED:
        return make_ready_future<frozen_schema_and_views>(get_frozen_with_views());
    case schema_registry_entry::view_state::UNMATCHED:
        _view_state = schema_registry_entry::view_state::MATCHING;
    case schema_registry_entry::view_state::MATCHING:
        return _views_matched_promise.get_shared_future().then([entry = shared_from_this()] {
            return entry->get_frozen_with_views();
        });
    }
    abort();
}

frozen_schema_and_views schema_registry_entry::get_frozen_with_views() const {
    auto views = boost::copy_range<std::vector<frozen_schema>>(_views | boost::adaptors::transformed([this] (auto&& v) {
        return v->frozen();
    }));
    return frozen_schema_and_views(frozen(), std::move(views));
}

future<schema_and_views> schema_registry_entry::get_with_views_eventually() {
    switch (_view_state) {
    case schema_registry_entry::view_state::MATCHED:
        return make_ready_future<schema_and_views>(get_with_views());
    case schema_registry_entry::view_state::UNMATCHED:
        _view_state = schema_registry_entry::view_state::MATCHING;
    case schema_registry_entry::view_state::MATCHING:
        return _views_matched_promise.get_shared_future().then([entry = shared_from_this()] {
            return entry->get_with_views();
        });
    }
    abort();
}

schema_and_views schema_registry_entry::get_with_views() {
    auto views = boost::copy_range<std::vector<view_ptr>>(_views | boost::adaptors::transformed([this] (auto&& v) {
        return view_ptr(v->get_schema());
    }));
    return schema_and_views{get_schema(), std::move(views)};
}

schema_registry& local_schema_registry() {
    return registry;
}

global_schema_ptr::global_schema_ptr(const global_schema_ptr& o)
    : global_schema_ptr(o.get())
{ }

global_schema_ptr::global_schema_ptr(global_schema_ptr&& o) {
    auto current = engine().cpu_id();
    if (o._cpu_of_origin != current) {
        throw std::runtime_error("Attempted to move global_schema_ptr across shards");
    }
    _ptr = std::move(o._ptr);
    _cpu_of_origin = current;
}

schema_ptr global_schema_ptr::get() const {
    if (engine().cpu_id() == _cpu_of_origin) {
        return _ptr;
    } else {
        // 'e' points to a foreign entry, but we know it won't be evicted
        // because _ptr is preventing this.
        const schema_registry_entry& e = *_ptr->registry_entry();
        schema_ptr s = local_schema_registry().get_or_null(e.version());
        if (!s) {
            s = local_schema_registry().get_or_load(e.version(), [&e](table_schema_version) {
                return e.frozen();
            });
            if (e.is_synced()) {
                s->registry_entry()->mark_synced();
            }
        }
        return s;
    }
}

global_schema_ptr::global_schema_ptr(const schema_ptr& ptr)
    : _ptr([&ptr]() {
        // _ptr must always have an associated registry entry,
        // if ptr doesn't, we need to load it into the registry.
        schema_registry_entry* e = ptr->registry_entry();
        if (e) {
            return ptr;
        }
        return local_schema_registry().get_or_load(ptr->version(), [&ptr] (table_schema_version) {
                return frozen_schema(ptr);
            });
        }())
    , _cpu_of_origin(engine().cpu_id())
{ }

global_schema_and_views::global_schema_and_views(const schema_and_views& sav)
        : schema(sav.schema)
        , views(boost::copy_range<std::vector<global_schema_ptr>>(sav.views | boost::adaptors::transformed([] (auto& view) {
              return global_schema_ptr(view);
          })))
{ }

schema_and_views global_schema_and_views::get() const {
    auto s = schema.get();
    auto vs = boost::copy_range<std::vector<view_ptr>>(views | boost::adaptors::transformed([] (auto& view) {
        return view_ptr(view.get());
    }));
    local_schema_registry().learn_views(s, vs);
    return schema_and_views{std::move(s), std::move(vs)};
}
