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

#include "range_tombstone.hh"
#include <seastar/util/defer.hh>

class range_tombstone_list final {
    using range_tombstones_type = boost::intrusive::set<range_tombstone,
        boost::intrusive::member_hook<range_tombstone, boost::intrusive::set_member_hook<>, &range_tombstone::_link>,
        boost::intrusive::compare<range_tombstone::compare>>;
    friend class range_tombstone;
private:
    range_tombstones_type _tombstones;
public:
    struct copy_comparator_only {};
    range_tombstone_list(const schema& s)
        : _tombstones(range_tombstone::compare(s))
    { }
    range_tombstone_list(const range_tombstone_list& x, copy_comparator_only)
        : _tombstones(x._tombstones.key_comp())
    { }
    range_tombstone_list(const range_tombstone_list&);
    range_tombstone_list& operator=(range_tombstone_list&) = delete;
    range_tombstone_list(range_tombstone_list&&) = default;
    range_tombstone_list& operator=(range_tombstone_list&&) = default;
    ~range_tombstone_list();
    size_t size() const {
        return _tombstones.size();
    }
    bool empty() const {
        return _tombstones.empty();
    }
    range_tombstones_type& tombstones() {
        return _tombstones;
    }
    auto begin() const {
        return _tombstones.begin();
    }
    auto end() const {
        return _tombstones.end();
    }
    void add(const schema& s, const range_tombstone& rt) {
        add(s, rt.start, rt.stop, rt.tomb);
    }
    void add(const schema& s, clustering_key_prefix start, clustering_key_prefix stop, tombstone tomb);
    tombstone search_tombstone_covering(const schema& s, const clustering_key& key) const;
    template <typename Func>
    void erase_where(Func filter) {
        auto it = begin();
        while (it != end()) {
            if (filter(*it)) {
                it = _tombstones.erase_and_dispose(it, current_deleter<range_tombstone>());
            } else {
                ++it;
            }
        }
    }
private:
    void insert_from(const schema& s, range_tombstones_type::iterator it, clustering_key_prefix start, clustering_key_prefix stop, tombstone tomb);
};
