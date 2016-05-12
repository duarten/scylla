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

#include "seastar/core/gate.hh"

namespace gms {
/**
 * A feature manager allows callers to check support for a particular feature
 * across all the nodes the current one is aware of.
 */
class feature_manager {
    seastar::gate _g;
public:
    // Returns a future which, when completed, indicates whether the specified
    // feature is supported. If stop() is called while a check is in progress,
    // the future is completed with false.
    future<bool> check_support_for(sstring feature);
    // Stops any checks in progress.
    future<> stop() {
        return _g.close();
    }
};
/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 */
class feature final {
    friend class gossiper;
    sstring _name;
    bool _enabled;
    explicit feature(feature_manager& manager, sstring name)
            : _name(std::move(name))
            , _enabled(false) {
        manager.check_support_for(_name).then([this](bool enabled) {
            _enabled = enabled;
        });
    }
public:
    const sstring& name() const {
        return _name;
    }
    explicit operator bool() const {
        return _enabled;
    }
    bool operator==(const feature& other) const {
        return _name == other._name;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const feature& f) {
        return os << "{ gossip feature = " << f._name << " }";
    }
};

} // namespace gms
