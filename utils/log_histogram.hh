/*
 * Copyright (C) 2017 ScyllaDB
 *
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

#include <boost/intrusive/slist.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <limits>
#include <seastar/core/bitops.hh>

inline constexpr size_t pow2_rank(size_t v) {
    return std::numeric_limits<size_t>::digits - 1 - count_leading_zeros(v);
}

template<size_t MinSizeShift, size_t SubBucketShift, size_t MaxSizeShift>
struct log_histogram_options {
    static constexpr size_t min_size_shift = MinSizeShift;
    static constexpr size_t sub_bucket_shift = SubBucketShift;
    static constexpr size_t max_size_shift = MaxSizeShift;
    static constexpr size_t number_of_buckets = ((MaxSizeShift - MinSizeShift) << SubBucketShift) + 2;
    using bucket_index_type = std::conditional_t<(number_of_buckets > ((1 << 16) - 1)), uint32_t,
          std::conditional_t<(number_of_buckets > ((1 << 8) - 1)), uint16_t, uint8_t>>;
};

template<typename Options>
struct log_histogram_hook : public bi::list_base_hook<> {
    typename Options::bucket_index_type cached_bucket;
};

template<typename T>
size_t hist_key(const T&);

template<typename T, typename Options, bool = std::is_base_of<log_histogram_hook<Options>, T>::value>
struct log_histogram_element_traits {
    using bucket_type = bi::list<T, bi::constant_time_size<false>>;
    static void cache_bucket(T& v, typename Options::bucket_index_type b) {
        v.cached_bucket = b;
    }
    static size_t cached_bucket(const T& v) {
        return v.cached_bucket;
    }
    static size_t hist_key(const T& v) {
        return ::hist_key<T>(v);
    }
};

template<typename T, typename Options>
struct log_histogram_element_traits<T, Options, false> {
    using bucket_type = typename T::bucket_type;
    static void cache_bucket(T&, typename Options::bucket_index_type);
    static size_t cached_bucket(const T&);
    static size_t hist_key(const T&);
};

/*
 * Histogram that stores elements in different buckets according to their size.
 * Values are mapped to a sequence of power-of-two ranges that are split in
 * 1 << SubbucketShift sub-buckets. Values less than 1 << MinSizeShift are placed
 * in bucket 0, whereas values bigger than 1 << MaxSizeShift are not admitted.
 * The histogram gives bigger precision to smaller values, with precision decreasing
 * as values get bigger.
 */
template<typename T, typename Options>
GCC6_CONCEPT(
    requires requires() {
        typename log_histogram_element_traits<T, Options>;
        size_t(Options::min_size_shift);
        size_t(Options::sub_bucket_shift);
        size_t(Options::max_size_shift);
        requires std::is_same<Options, log_histogram_options<Options::min_size_shift, Options::sub_bucket_shift, Options::max_size_shift>>::value;
    }
)
class log_histogram final {
private:
    static constexpr size_t number_of_buckets = Options::number_of_buckets;
    using traits = log_histogram_element_traits<T, Options>;
    using bucket = typename traits::bucket_type;

    struct hist_size_less_compare {
        inline bool operator()(const T& v1, const T& v2) const {
            return traits::hist_key(v1) < traits::hist_key(v2);
        }
    };

    std::array<bucket, number_of_buckets> _buckets;
    ssize_t _watermark = -1;
public:
    template <bool IsConst>
    class hist_iterator : public std::iterator<std::input_iterator_tag, std::conditional_t<IsConst, const T, T>> {
        using hist_type = std::conditional_t<IsConst, const log_histogram, log_histogram>;
        using iterator_type = std::conditional_t<IsConst, typename bucket::const_iterator, typename bucket::iterator>;

        hist_type& _h;
        ssize_t _b;
        iterator_type _it;
    public:
        struct end_tag {};
        hist_iterator(hist_type& h)
            : _h(h)
            , _b(h._watermark)
            , _it(_b >= 0 ? h._buckets[_b].begin() : h._buckets[0].end()) {
        }
        hist_iterator(hist_type& h, end_tag)
            : _h(h)
            , _b(-1)
            , _it(h._buckets[0].end()) {
        }
        std::conditional_t<IsConst, const T, T>& operator*() {
            return *_it;
        }
        hist_iterator& operator++() {
            if (++_it == _h._buckets[_b].end()) {
                do {
                    --_b;
                } while (_b >= 0 && (_it = _h._buckets[_b].begin()) == _h._buckets[_b].end());
            }
            return *this;
        }
        bool operator==(const hist_iterator& other) const {
            return _b == other._b && _it == other._it;
        }
        bool operator!=(const hist_iterator& other) const {
            return !(*this == other);
        }
    };
    using iterator = hist_iterator<false>;
    using const_iterator = hist_iterator<true>;
public:
    bool empty() const {
        return _watermark == -1;
    }
    bool contains_above_min() const {
        return _watermark > 0;
    }
    const_iterator begin() const {
        return const_iterator(*this);
    }
    const_iterator end() const {
        return const_iterator(*this, typename const_iterator::end_tag());
    }
    iterator begin() {
        return iterator(*this);
    }
    iterator end() {
        return iterator(*this, typename iterator::end_tag());
    }
    // Pops one of the smallest elements in the histogram.
    void pop_one_of_smallest() {
        _buckets[_watermark].pop_front();
        maybe_adjust_watermark();
    }
    // Returns one of the smallest elements in the histogram.
    const T& one_of_smallest() const {
        return _buckets[_watermark].front();
    }
    // Returns the smallest element in the histogram.
    // Too expensive to be called from anything other than tests.
    const T& smallest() const {
        return *boost::max_element(_buckets[_watermark], hist_size_less_compare());
    }
    // Returns one of the smallest elements in the histogram.
    T& one_of_smallest() {
        return _buckets[_watermark].front();
    }
    // Returns the smallest element in the histogram.
    // Too expensive to be called from anything other than tests.
    T& smallest() {
        return *boost::max_element(_buckets[_watermark], hist_size_less_compare());
    }
    // Pushes a new element onto the histogram.
    void push(T& v) {
        auto b = bucket_of(traits::hist_key(v));
        traits::cache_bucket(v, b);
        _buckets[b].push_front(v);
        _watermark = std::max(ssize_t(b), _watermark);
    }
    // Adjusts the histogram when the specified element becomes bigger.
    void adjust_up(T& v) {
        auto b = traits::cached_bucket(v);
        auto nb = bucket_of(traits::hist_key(v));
        if (nb != b) {
            traits::cache_bucket(v, nb);
            _buckets[nb].splice(_buckets[nb].begin(), _buckets[b], _buckets[b].iterator_to(v));
            _watermark = std::max(ssize_t(nb), _watermark);
        }
    }
    // Removes the specified element from the histogram.
    void erase(T& v) {
        auto& b = _buckets[traits::cached_bucket(v)];
        b.erase(b.iterator_to(v));
        maybe_adjust_watermark();
    }
    // Merges the specified histogram, moving all elements from it into this.
    void merge(log_histogram& other) {
        for (size_t i = 0; i < number_of_buckets; ++i) {
            _buckets[i].splice(_buckets[i].begin(), other._buckets[i]);
        }
        _watermark = std::max(_watermark, other._watermark);
        other._watermark = -1;
    }
private:
    void maybe_adjust_watermark() {
        while (_buckets[_watermark].empty() && --_watermark >= 0) ;
    }
    static typename Options::bucket_index_type bucket_of(size_t value) {
        const auto pow2_index = pow2_rank(value | (1 << (Options::min_size_shift - 1)));
        const auto unmasked_sub_bucket_index = value >> (pow2_index - Options::sub_bucket_shift);
        const auto bucket = pow2_index - Options::min_size_shift + 1;
        const auto bigger = value >= (1 << Options::min_size_shift);
        const auto mask = ((1 << Options::sub_bucket_shift) - 1) & -bigger;
        const auto sub_bucket_index = unmasked_sub_bucket_index & mask;
        return (bucket << Options::sub_bucket_shift) - mask + sub_bucket_index;
    }
};
