/*
 * Copyright (C) 2017 ScyllaDB
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

#include <seastar/util/gcc6-concepts.hh>

GCC6_CONCEPT(
template<typename T>
concept bool HasTriCompare =
    requires(const T& t) {
        { t.compare(t) const } -> int;
    };
)

template<typename T>
GCC6_CONCEPT( requires HasTriCompare<T> )
class generate_inequality_from_tri_compare {
    int do_compare(const T& t) const {
        return static_cast<const T*>(this)->compare(t);
    }

public:
    bool operator<(const T& t) const {
        return do_compare(t) < 0;
    }

    bool operator<=(const T& t) const {
        return do_compare(t) <= 0;
    }

    bool operator>(const T& t) const {
        return do_compare(t) > 0;
    }

    bool operator>=(const T& t) const {
        return do_compare(t) >= 0;
    }

    bool operator==(const T& t) const {
        return do_compare(t) == 0;
    }

    bool operator!=(const T& t) const {
        return do_compare(t) != 0;
    }
};
