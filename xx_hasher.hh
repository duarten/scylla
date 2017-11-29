/*
 * Copyright (C) 2018 ScyllaDB
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
#include "utils/serialization.hh"

#include <xxHash/xxhash.h>
#include <memory>

class xx_hasher {
    static constexpr size_t digest_size = 16;

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
        return operator=(xx_hasher(other));
    }

    void update(const char* ptr, size_t length) {
        XXH64_update(_state.get(), ptr, length);
    }

    bytes finalize() {
        bytes digest{bytes::initialized_later(), digest_size};
        serialize_to(digest.begin());
        return digest;
    }

    std::array<uint8_t, 16> finalize_array() {
        std::array<uint8_t, 16> digest;
        serialize_to(digest.begin());
        return digest;
    };

    uint64_t finalize_uint64() {
        return XXH64_digest(_state.get());
    }

private:
    template<typename OutIterator>
    void serialize_to(OutIterator&& out) {
        serialize_int64(out, 0);
        serialize_int64(out, finalize_uint64());
    }
};
