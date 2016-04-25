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
 * Represents the kind of bound in a range tombstone. The special `clustering`
 * value represents a clustering key prefix, against which different types of
 * bounds compare differentely.
 */
enum class bound_kind : uint8_t {
    excl_end_bound,
    incl_start_bound,
    clustering,
    incl_end_bound,
    excl_start_bound,
};

std::ostream& operator<<(std::ostream& out, const bound_kind k);

struct bound_kind_compare {
    int32_t comparison(bound_kind k) const {
        switch(k) {
        case bound_kind::excl_end_bound:
        case bound_kind::incl_start_bound:
            return 0;
        case bound_kind::clustering:
            return 2;
        case bound_kind::incl_end_bound:
        case bound_kind::excl_start_bound:
            return 3;
        }
        assert(false); // never reached
    }
    bool operator()(bound_kind k1, bound_kind k2) const {
        return comparison(k1) < comparison(k2);
    }
};

bound_kind invert_kind(bound_kind k);

class bound {
    static const clustering_key_prefix empty_prefix;
public:
    const clustering_key_prefix& prefix;
    bound_kind kind;
    bound(const clustering_key_prefix& prefix, bound_kind kind = bound_kind::clustering)
        : prefix(prefix)
        , kind(kind)
    { }
    struct compare {
        lw_shared_ptr<compound_type<allow_prefixes::yes>> _prefix_type;
        compare(const schema& s) : _prefix_type(s.clustering_key_prefix_type()) {}
        int32_t compared_to_clustering(bound_kind k) const {
            switch(k) {
            case bound_kind::excl_end_bound:
            case bound_kind::incl_start_bound:
                return -1;
            case bound_kind::clustering:
                return 0;
            case bound_kind::incl_end_bound:
            case bound_kind::excl_start_bound:
                return 1;
            }
            assert(false); // never reached
        }
        bool operator()(const clustering_key_prefix& p1, bound_kind k1, const clustering_key_prefix& p2, bound_kind k2) const {
            auto res = prefix_equality_tri_compare(_prefix_type->types().begin(),
                _prefix_type->begin(p1), _prefix_type->end(p1),
                _prefix_type->begin(p2), _prefix_type->end(p2),
                tri_compare);
            if (res) {
                return res < 0;
            }

            auto d1 = std::distance(_prefix_type->begin(p1), _prefix_type->end(p1));
            auto d2 = std::distance(_prefix_type->begin(p2), _prefix_type->end(p2));
            if (d1 == d2) {
                return bound_kind_compare()(k1, k2);
            }

            return d1 < d2 ? compared_to_clustering(k1) < 0 : compared_to_clustering(k2) > 0;
        }
        bool operator()(const bound b, const clustering_key_prefix& p) const {
            return operator()(b.prefix, b.kind, p, bound_kind::clustering);
        }
        bool operator()(const clustering_key_prefix& p, const bound b) const {
            return operator()(p, bound_kind::clustering, b.prefix, b.kind);
        }
        bool operator()(const bound b1, const bound b2) const {
            return operator()(b1.prefix, b1.kind, b2.prefix, b2.kind);
        }
    };
    bool equal(const schema& s, const bound other) const {
        return prefix.equal(s, other.prefix) && kind == other.kind;
    }
    static bound bottom() {
        return {empty_prefix, bound_kind::incl_start_bound};
    }
    static bound top() {
        return {empty_prefix, bound_kind::incl_end_bound};
    }
    friend std::ostream& operator<<(std::ostream& out, const bound& b) {
        return out << "{bound: prefix=" << b.prefix << ", kind=" << b.kind << "}";
    }
};

/**
 * Represents a ranged deletion operation. Can be empty.
 */
class range_tombstone final {
    boost::intrusive::set_member_hook<> _link;
    friend class range_tombstone_list;
public:
    clustering_key_prefix start;
    bound_kind start_kind;
    clustering_key_prefix end;
    bound_kind end_kind;
    tombstone tomb;
    range_tombstone(bound start, bound end, tombstone tomb)
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
        , start_kind(bound_kind::incl_start_bound)
        , end(std::move(end))
        , end_kind(bound_kind::incl_end_bound)
        , tomb(std::move(tomb))
    { }
    range_tombstone()
        : start(std::vector<bytes>())
        , start_kind(bound_kind::incl_start_bound)
        , end(std::vector<bytes>())
        , end_kind(bound_kind::incl_end_bound)
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
    const bound start_bound() const {
        return bound(start, start_kind);
    }
    const bound end_bound() const {
        return bound(end, end_kind);
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
        bound::compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const range_tombstone& rt1, const range_tombstone& rt2) const {
            return _c(rt1.start_bound(), rt2.start_bound());
        }
    };
    template<typename Hasher>
    void feed_hash(Hasher& h, const schema& s) const {
        start.feed_hash(h, s);
        if (!start.equal(s, end)) {
            ::feed_hash(h, start_kind);
            end.feed_hash(h, s);
            ::feed_hash(h, end_kind);
        }
        ::feed_hash(h, tomb);
    }
    friend std::ostream& operator<<(std::ostream& out, const range_tombstone& rt);
};
