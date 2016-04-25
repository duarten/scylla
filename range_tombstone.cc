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

#include "range_tombstone.hh"

range_tombstone::range_tombstone(range_tombstone&& rt) noexcept
    : start(std::move(rt.start))
    , stop(std::move(rt.stop))
    , tomb(std::move(rt.tomb)) {
}

std::ostream& operator<<(std::ostream& out, const range_tombstone& rt) {
    if (rt) {
        return out << "{range_tombstone: start=" << rt.start << ", stop=" << rt.stop << ", " << rt.tomb << "}";
    } else {
        return out << "{range_tombstone: none}";
    }
}
