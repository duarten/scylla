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
