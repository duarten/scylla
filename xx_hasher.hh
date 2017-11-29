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

#include "bytes.hh"
#include <xxHash/xxhash.h>
#include <memory>

class xx_hasher {
    struct xxhash_state_deleter {
        void operator()(XXH64_state_t* x) const { XXH64_freeState(x); }
    };

    std::unique_ptr<XXH64_state_t, xxhash_state_deleter> _state;
public:
    xx_hasher() : _state(std::unique_ptr<XXH64_state_t, xxhash_state_deleter>(XXH64_createState())) {
        XXH64_reset(_state.get(), 0);
    }

    xx_hasher(xx_hasher&&) noexcept = default;

    xx_hasher& operator=(xx_hasher&&) noexcept = default;

    xx_hasher(const xx_hasher& other) : xx_hasher() {
        XXH64_copyState(_state.get(), other._state.get());
    }

    xx_hasher& operator=(const xx_hasher& other) {
        if (this != &other) {
            auto tmp = other;
            this->~xx_hasher();
            new (this) xx_hasher(std::move(tmp));
        }
        return *this;
    }

    void update(const char* ptr, size_t length) {
        XXH64_update(_state.get(), ptr, length);
    }

    uint64_t finalize() {
        return XXH64_digest(_state.get());
    }
};
