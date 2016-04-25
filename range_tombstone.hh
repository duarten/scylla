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

#include <boost/intrusive/set.hpp>
#include "hashing.hh"
#include "keys.hh"
#include "tombstone.hh"

/**
 * Represents a ranged deletion operation. Can be empty.
 */
class range_tombstone final {
    boost::intrusive::set_member_hook<> _link;
    friend class range_tombstone_list;
public:
    clustering_key_prefix start;
    clustering_key_prefix stop;
    tombstone tomb;

    range_tombstone(const clustering_key_prefix& start, const clustering_key_prefix& stop, tombstone tomb)
        : _link()
        , start(start)
        , stop(stop)
        , tomb(std::move(tomb))
    { }
    range_tombstone(clustering_key_prefix&& start, clustering_key_prefix&& stop, tombstone tomb)
        : _link()
        , start(std::move(start))
        , stop(std::move(stop))
        , tomb(std::move(tomb))
    { }
    range_tombstone()
        : _link()
        , start(std::vector<bytes>())
        , stop(std::vector<bytes>())
        , tomb()
    { }
    range_tombstone(range_tombstone&& rt) noexcept;
    range_tombstone(const range_tombstone&) = default;
    range_tombstone& operator=(const range_tombstone&) = default;
    bool empty() const {
        return !bool(tomb);
    }
    explicit operator bool() const {
        return bool(tomb);
    }
    bool equal(const schema& s, const range_tombstone& other) const {
        return tomb == other.tomb && start.equal(s, other.start) && stop.equal(s, other.stop);
    }
    template<typename Hasher>
	void feed_hash(Hasher& h, const schema& s) const {
		start.feed_hash(h, s);
		stop.feed_hash(h, s);
		::feed_hash(h, tomb);
	}
    struct compare { // Used mainly for lower_bound operations on an intrusive set which includes this range_tombstone.
        clustering_key_prefix::less_compare _c;
        compare(const schema& s) : _c(s) { }
        bool operator()(const range_tombstone& rt1, const range_tombstone& rt2) const {
            return _c(rt1.stop, rt2.stop);
        }
        bool operator()(const clustering_key_prefix& prefix, const range_tombstone& rt) const {
            return _c(prefix, rt.stop);
        }
        bool operator()(const range_tombstone& rt, const clustering_key_prefix& prefix) const {
            return _c(rt.stop, prefix);
        }
    };
    friend std::ostream& operator<<(std::ostream& out, const range_tombstone& rt);
};
