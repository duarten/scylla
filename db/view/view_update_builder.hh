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

#include "clustering_bounds_comparator.hh"
#include "range_tombstone.hh"
#include "streamed_mutation.hh"
#include "tombstone.hh"

namespace db {

namespace view {

class tombstone_tracker final {
    bound_view::compare _cmp;
    tombstone  _partition_tombstone;
    stdx::optional<range_tombstone> _current_range_tombstone;
public:
    explicit tombstone_tracker(const schema& s, tombstone partition_tombstone)
            : _cmp(s)
            , _partition_tombstone(std::move(partition_tombstone)) {
    }

    void apply(range_tombstone&& rt);

    tombstone current_tombstone() const;

    // The rows passed to apply_to() must be in clustering order.
    void apply_to(clustering_row& row);
};

}

}
