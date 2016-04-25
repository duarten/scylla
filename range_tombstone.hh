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
 * Represents the kind of bound in a range tombstone.
 */
enum class bound_kind : uint8_t {
    excl_end = 0,
    incl_start = 1,
    // values 2 to 5 are reserved for forward Origin compatibility
    incl_end = 6,
    excl_start = 7,
};

std::ostream& operator<<(std::ostream& out, const bound_kind k);

bound_kind invert_kind(bound_kind k);

class bound_view {
    const static thread_local clustering_key empty_prefix;
public:
    const clustering_key_prefix& prefix;
    bound_kind kind;
    bound_view(const clustering_key_prefix& prefix, bound_kind kind)
        : prefix(prefix)
        , kind(kind)
    { }
    struct compare {
        // To make it assignable and to avoid taking a schema_ptr, we
        // wrap the schema reference.
        std::reference_wrapper<const schema> _s;
        compare(const schema& s) : _s(s)
        { }
        int32_t weight(bound_kind k) const {
            switch(k) {
            case bound_kind::excl_end:
            case bound_kind::incl_start:
                return -1;
            case bound_kind::incl_end:
            case bound_kind::excl_start:
                return 1;
            }
            abort();
        }
        bool operator()(const clustering_key_prefix& p1, int32_t w1, const clustering_key_prefix& p2, int32_t w2) const {
            auto type = _s.get().clustering_key_prefix_type();
            auto res = prefix_equality_tri_compare(type->types().begin(),
                type->begin(p1), type->end(p1),
                type->begin(p2), type->end(p2),
                tri_compare);
            if (res) {
                return res < 0;
            }
            auto d1 = p1.size(_s);
            auto d2 = p2.size(_s);
            if (d1 == d2) {
                return w1 < w2;
            }
            return d1 < d2 ? w1 < 0 : w2 > 0;
        }
        bool operator()(const bound_view b, const clustering_key_prefix& p) const {
            return operator()(b.prefix, weight(b.kind), p, 0);
        }
        bool operator()(const clustering_key_prefix& p, const bound_view b) const {
            return operator()(p, 0, b.prefix, weight(b.kind));
        }
        bool operator()(const bound_view b1, const bound_view b2) const {
            return operator()(b1.prefix, weight(b1.kind), b2.prefix, weight(b2.kind));
        }
    };
    bool equal(const schema& s, const bound_view other) const {
        return kind == other.kind && prefix.equal(s, other.prefix);
    }
    static bound_view bottom() {
        return {empty_prefix, bound_kind::incl_start};
    }
    static bound_view top() {
        return {empty_prefix, bound_kind::incl_end};
    }
    friend std::ostream& operator<<(std::ostream& out, const bound_view& b) {
        return out << "{bound: prefix=" << b.prefix << ", kind=" << b.kind << "}";
    }
};

/**
 * Represents a ranged deletion operation. Can be empty.
 */
class range_tombstone final {
    boost::intrusive::set_member_hook<> _link;
public:
    clustering_key_prefix start;
    bound_kind start_kind;
    clustering_key_prefix end;
    bound_kind end_kind;
    tombstone tomb;
    range_tombstone(bound_view start, bound_view end, tombstone tomb)
        : start(start.prefix)
        , start_kind(start.kind)
        , end(end.prefix)
        , end_kind(end.kind)
        , tomb(std::move(tomb))
    { }
    range_tombstone(clustering_key_prefix start, bound_kind start_kind, clustering_key_prefix end, bound_kind end_kind, tombstone tomb)
        : start(std::move(start))
        , start_kind(start_kind)
        , end(std::move(end))
        , end_kind(end_kind)
        , tomb(std::move(tomb))
    { }
    range_tombstone(clustering_key_prefix&& start, clustering_key_prefix&& end, tombstone tomb)
        : start(std::move(start))
        , start_kind(bound_kind::incl_start)
        , end(std::move(end))
        , end_kind(bound_kind::incl_end)
        , tomb(std::move(tomb))
    { }
    range_tombstone(range_tombstone&& rt) noexcept
        : start(std::move(rt.start))
        , start_kind(rt.start_kind)
        , end(std::move(rt.end))
        , end_kind(rt.end_kind)
        , tomb(std::move(rt.tomb))
    { }
    range_tombstone& operator=(range_tombstone&& rt) noexcept {
        start = std::move(rt.start);
        start_kind = rt.start_kind;
        end = std::move(rt.end);
        end_kind = rt.end_kind;
        tomb = std::move(rt.tomb);
        return *this;
    }
    range_tombstone(const range_tombstone& rt) = default;
    range_tombstone& operator=(const range_tombstone&) = default;
    // IDL constructor
    range_tombstone(clustering_key_prefix&& start, tombstone tomb, bound_kind start_kind, clustering_key_prefix&& end, bound_kind end_kind)
        : start(std::move(start))
        , start_kind(start_kind)
        , end(std::move(end))
        , end_kind(end_kind)
        , tomb(std::move(tomb))
    { }
    const bound_view start_bound() const {
        return bound_view(start, start_kind);
    }
    const bound_view end_bound() const {
        return bound_view(end, end_kind);
    }
    bool empty() const {
        return !bool(tomb);
    }
    explicit operator bool() const {
        return bool(tomb);
    }
    bool equal(const schema& s, const range_tombstone& other) const {
        return tomb == other.tomb && start_bound().equal(s, other.start_bound()) && end_bound().equal(s, other.end_bound());
    }
    struct compare {
        bound_view::compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const range_tombstone& rt1, const range_tombstone& rt2) const {
            return _c(rt1.start_bound(), rt2.start_bound());
        }
    };
    template<typename Hasher>
    void feed_hash(Hasher& h, const schema& s) const {
        start.feed_hash(h, s);
        // For backward compatibility, don't consider new fields if
        // this could be an old-style, overlapping, range tombstone.
        if (!start.equal(s, end) || start_kind != bound_kind::incl_start || end_kind != bound_kind::incl_end) {
            ::feed_hash(h, start_kind);
            end.feed_hash(h, s);
            ::feed_hash(h, end_kind);
        }
        ::feed_hash(h, tomb);
    }
    friend std::ostream& operator<<(std::ostream& out, const range_tombstone& rt);
};
