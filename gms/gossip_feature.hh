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

#include "gms/gossiper.hh"

namespace gms {
/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 */
class gossip_feature final {
    sstring _name;
    bool _enabled;
    seastar::gate _g;
public:
    explicit gossip_feature(sstring name)
            : _name(std::move(name))
            , _enabled(false) { }
    void check_support() {
         get_local_gossiper().wait_for_feature_on_all_node({_name}, _g).then([this] {
            _enabled = true;
        }).handle_exception([](auto&& ep) {
            try {
                std::rethrow_exception(ep);
            } catch (seastar::gate_closed_exception& ignored) { }
        });
    }
    const sstring& name() const {
        return _name;
    }
    explicit operator bool() const {
        return _enabled;
    }
    future<> stop() {
        return _g.close();
    }
    bool operator==(const gossip_feature& other) const {
        return _name == other._name;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const gossip_feature& f) {
        return os << "{ gossip feature = " << f._name << " }";
    }
};

} // namespace gms
