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

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <random>
#include <iostream>
#include "keys.hh"
#include "schema_builder.hh"
#include "range_tombstone_list.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

static schema_ptr s = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v", int32_type, column_kind::regular_column)
        .build();

static auto gc_now = gc_clock::now();

template <typename... T>
static clustering_key_prefix key(T... components) {
    std::vector<bytes> exploded{int32_type->decompose(components...)};
    return clustering_key_prefix::from_clustering_prefix(*s, exploded_clustering_prefix(std::move(exploded)));
}

static range_tombstone rt(int32_t start, int32_t stop, api::timestamp_type timestamp) {
	return range_tombstone(key(start), key(stop), {timestamp, gc_now});
}

static void assert_rt(range_tombstone expected, range_tombstone actual) {
	BOOST_REQUIRE(expected.equal(*s, actual));
}

BOOST_AUTO_TEST_CASE(test_sorted_addition) {
    range_tombstone_list l(*s);

    auto rt1 = rt(1, 5, 3);
    auto rt2 = rt(7, 10, 2);
    auto rt3 = rt(10, 13, 1);

    l.add(*s, rt1);
    l.add(*s, rt2);
    l.add(*s, rt3);

    auto it = l.begin();
    assert_rt(rt1, *it++);
    assert_rt(rt2, *it++);
    assert_rt(rt3, *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_non_sorted_addition) {
    range_tombstone_list l(*s);

    auto rt1 = rt(1, 5, 3);
    auto rt2 = rt(7, 10, 2);
    auto rt3 = rt(10, 13, 1);

    l.add(*s, rt2);
    l.add(*s, rt1);
    l.add(*s, rt3);

    auto it = l.begin();
    assert_rt(rt1, *it++);
    assert_rt(rt2, *it++);
    assert_rt(rt3, *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_overlapping_addition) {
    range_tombstone_list l(*s);

	l.add(*s, rt(4, 10, 3));
	l.add(*s, rt(1, 7, 2));
	l.add(*s, rt(8, 13, 4));
	l.add(*s, rt(0, 15, 1));

	auto it = l.begin();
	assert_rt(rt(0, 1, 1), *it++);
	assert_rt(rt(1, 4, 2), *it++);
	assert_rt(rt(4, 8, 3), *it++);
	assert_rt(rt(8, 13, 4), *it++);
	assert_rt(rt(13, 15, 1), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_simple_overlap) {
    range_tombstone_list l1(*s);

	l1.add(*s, rt(0, 10, 3));
	l1.add(*s, rt(3, 7, 5));

    auto it = l1.begin();
    assert_rt(rt(0, 3, 3), *it++);
    assert_rt(rt(3, 7, 5), *it++);
    assert_rt(rt(7, 10, 3), *it++);
    BOOST_REQUIRE(it == l1.end());

    range_tombstone_list l2(*s);

	l2.add(*s, rt(0, 10, 3));
	l2.add(*s, rt(3, 7, 2));

    it = l2.begin();
    assert_rt(rt(0, 10, 3), *it++);
    BOOST_REQUIRE(it == l2.end());
}

BOOST_AUTO_TEST_CASE(test_overlapping_previous_end_equals_start) {
    range_tombstone_list l(*s);

	l.add(*s, rt(11, 12, 2));
	l.add(*s, rt(1, 4, 2));
	l.add(*s, rt(4, 10, 5));

	BOOST_REQUIRE(2 == l.search_tombstone_covering(*s, key(3)).timestamp);
	BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key(4)).timestamp);
	BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key(8)).timestamp);
	BOOST_REQUIRE(3 == l.size());
}

BOOST_AUTO_TEST_CASE(test_search) {
    range_tombstone_list l(*s);

	l.add(*s, rt(0, 4, 5));
	l.add(*s, rt(4, 6, 2));
	l.add(*s, rt(9, 12, 1));
	l.add(*s, rt(14, 15, 3));
	l.add(*s, rt(15, 17, 6));

	BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key(-1)));

	BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key(0)).timestamp);
	BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key(3)).timestamp);
	BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key(4)).timestamp);

	BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key(18)));

	BOOST_REQUIRE(3 == l.search_tombstone_covering(*s, key(14)).timestamp);

	BOOST_REQUIRE(6 == l.search_tombstone_covering(*s, key(15)).timestamp);

	BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key(18)));
}

BOOST_AUTO_TEST_CASE(test_add_same) {
    range_tombstone_list l(*s);

	l.add(*s, rt(4, 4, 5));
	l.add(*s, rt(4, 4, 6));
	l.add(*s, rt(4, 4, 4));

    auto it = l.begin();
    assert_rt(rt(4, 4, 6), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_single_range_is_preserved) {
    range_tombstone_list l(*s);

	l.add(*s, rt(1, 2, 10));
    l.add(*s, rt(7, 13, 8));
    l.add(*s, rt(13, 13, 20));
    l.add(*s, rt(13, 18, 12));

    auto it = l.begin();
    assert_rt(rt(1, 2, 10), *it++);
    assert_rt(rt(7, 13, 8), *it++);
    assert_rt(rt(13, 13, 20), *it++);
    assert_rt(rt(13, 18, 12),  *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_single_range_is_replaced) {
    range_tombstone_list l(*s);

    l.add(*s, rt(7, 13, 8));
    l.add(*s, rt(13, 13, 20));
    l.add(*s, rt(13, 18, 32));

    auto it = l.begin();
    assert_rt(rt(7, 13, 8), *it++);
    assert_rt(rt(13, 18, 32),  *it++);
    BOOST_REQUIRE(it == l.end());
}

static bool assert_valid(range_tombstone_list& l) {
    int32_t prev_start = -2;
    int32_t prev_stop = -1;
    for (auto&& rt : l) {
        auto cur_start = value_cast<int32_t>(int32_type->deserialize(rt.start.explode().at(0)));
        auto cur_stop = value_cast<int32_t>(int32_type->deserialize(rt.stop.explode().at(0)));

        if (prev_stop > cur_start
            || cur_start > cur_stop
            || (cur_start == cur_stop && prev_stop == cur_start && prev_start == prev_stop)) {
            std::cout << "Invalid range tombstone list at " << rt << std::endl;
            return false;
        }

        prev_start = cur_start;
        prev_stop = cur_stop;
    }
    return true;
}

static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<int32_t> dist(0, 50);

static std::vector<range_tombstone> make_random() {
    std::vector<range_tombstone> rts;

	int32_t prev_start = -1;
	int32_t prev_stop = 0;
    int32_t size = dist(gen) + 7;
	for (int32_t i = 0; i < size; ++i) {
		int32_t next_start = prev_stop + dist(gen);
		int32_t next_stop = next_start + dist(gen);

		// We can have an interval [x, x], but not 2 consecutives ones for the same x
		if (next_stop == next_start && prev_stop == prev_start && prev_stop == next_start) {
			next_stop += 1 + dist(gen);
		}

		rts.emplace_back(rt(next_start, next_stop, dist(gen)));

		prev_start = next_start;
		prev_stop = next_stop;
    }
	return rts;
}

BOOST_AUTO_TEST_CASE(test_add_random) {
	for (uint32_t i = 0; i < 1000; ++i) {
		auto input = make_random();
		range_tombstone_list l(*s);
		for (auto&& rt : input) {
			l.add(*s, rt);
		}
		if (!assert_valid(l)) {
			std::cout << "For input:" << std::endl;
			for (auto&& rt : input) {
				std::cout << rt << std::endl;
			}
			std::cout << "Produced:" << std::endl;
			for (auto&& rt : l) {
				std::cout << rt << std::endl;
			}
			BOOST_REQUIRE(false);
		}
	}
}
