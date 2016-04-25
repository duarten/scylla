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

#include "range_tombstone_list.hh"

range_tombstone::range_tombstone(range_tombstone&& rt) noexcept
    : _link()
    , start(std::move(rt.start))
    , stop(std::move(rt.stop))
    , tomb(std::move(rt.tomb)) {
    using container_type = range_tombstone_list::range_tombstones_type;
    container_type::node_algorithms::replace_node(rt._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(rt._link.this_ptr());
}

std::ostream& operator<<(std::ostream& out, const range_tombstone& rt) {
    if (rt) {
        return out << "{range_tombstone: start=" << rt.start << ", stop=" << rt.stop << ", " << rt.tomb << "}";
    } else {
        return out << "{range_tombstone: none}";
    }
}
