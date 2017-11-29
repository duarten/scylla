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

#include "digest_algorithm.hh"
#include "md5_hasher.hh"
#include "utils/serialization.hh"
#include "xx_hasher.hh"

namespace query {

class digester final {
    static_assert(md5_hasher::size == digest_size, "MD5 hash size needs to match the digest size");

    struct xx_hasher_wrapper : public xx_hasher {
        bytes finalize() {
            bytes digest{bytes::initialized_later(), digest_size};
            serialize_to(digest.begin());
            return digest;
        }

        std::array<uint8_t, digest_size> finalize_array() {
            std::array<uint8_t, digest_size> digest;
            serialize_to(digest.begin());
            return digest;
        }

        template<typename OutIterator>
        void serialize_to(OutIterator&& out) {
            serialize_int64(out, 0);
            serialize_int64(out, xx_hasher::finalize());
        }
    };

    union concrete_hasher {
        md5_hasher md5;
        xx_hasher_wrapper xx;

        concrete_hasher() { }
        ~concrete_hasher() { }
    } _impl;
    digest_algorithm _algo;
public:
    explicit digester(digest_algorithm algo)
            : _algo(algo) {
        switch (_algo) {
        case digest_algorithm::MD5:
            new (&_impl.md5) md5_hasher();
            break;
        case digest_algorithm::xxHash:
            new (&_impl.xx) xx_hasher();
            break;
        case digest_algorithm ::none:
            break;
        }
    }

    ~digester() {
        switch (_algo) {
        case digest_algorithm::MD5:
            _impl.md5.~md5_hasher();
            break;
        case digest_algorithm::xxHash:
            _impl.xx.~xx_hasher();
            break;
        case digest_algorithm ::none:
            break;
        }
    }

    digester(digester&& other) noexcept
            : _algo(other._algo) {
        switch (_algo) {
        case digest_algorithm::MD5:
            new (&_impl.md5) md5_hasher(std::move(other._impl.md5));
            break;
        case digest_algorithm::xxHash:
            new (&_impl.xx) xx_hasher(std::move(other._impl.xx));
            break;
        case digest_algorithm ::none:
            break;
        }
    }

    digester(const digester& other) noexcept
            : _algo(other._algo) {
        switch (_algo) {
        case digest_algorithm::MD5:
            new (&_impl.md5) md5_hasher(other._impl.md5);
            break;
        case digest_algorithm::xxHash:
            new (&_impl.xx) xx_hasher(other._impl.xx);
            break;
        case digest_algorithm ::none:
            break;
        }
    }

    digester& operator=(digester&& other) noexcept {
        if (this != &other) {
            this->~digester();
            new (this) digester(std::move(other));
        }
        return *this;
    }

    digester& operator=(const digester& other) noexcept {
        if (this != &other) {
            auto tmp = other;
            this->~digester();
            new (this) digester(std::move(tmp));
        }
        return *this;
    }

    void update(const char* ptr, size_t length) {
        switch (_algo) {
        case digest_algorithm::MD5:
            _impl.md5.update(ptr, length);
            break;
        case digest_algorithm::xxHash:
            _impl.xx.update(ptr, length);
            break;
        case digest_algorithm ::none:
            break;
        }
    }

    bytes finalize() {
        switch (_algo) {
        case digest_algorithm::MD5:
            return _impl.md5.finalize();
        case digest_algorithm::xxHash:
            return _impl.xx.finalize();
        case digest_algorithm ::none:
            return bytes();
        }
        abort();
    }

    std::array<uint8_t, digest_size> finalize_array() {
        switch (_algo) {
        case digest_algorithm::MD5:
            return _impl.md5.finalize_array();
        case digest_algorithm::xxHash:
            return _impl.xx.finalize_array();
        case digest_algorithm ::none:
            return { };
        }
        abort();
    }
};

}