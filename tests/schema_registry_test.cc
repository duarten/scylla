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


#include <seastar/core/thread.hh>

#include "tests/test_services.hh"
#include "tests/test-utils.hh"
#include "schema_registry.hh"
#include "schema_builder.hh"
#include "mutation_source_test.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

static bytes random_column_name() {
    return to_bytes(to_hex(make_blob(32)));
}

static schema_ptr random_schema() {
    return schema_builder("ks", "cf")
           .with_column("pk", bytes_type, column_kind::partition_key)
           .with_column(random_column_name(), bytes_type)
           .build();
}

static schema_ptr random_view(schema_ptr base) {
    return schema_builder("ks", "cf")
           .with_column(*base->partition_key_columns().begin())
           .with_column(base->regular_begin()->name(), bytes_type, column_kind::partition_key)
           .with_view_info(*base, false, sstring())
           .build();
}

SEASTAR_TEST_CASE(test_async_loading) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s1 = random_schema();
        auto s2 = random_schema();

        auto s1_loaded = local_schema_registry().get_or_load(s1->version(), [s1] (table_schema_version) {
            return make_ready_future<frozen_schema_and_views>(frozen_schema_and_views(frozen_schema(s1), { }));
        }).get0();

        BOOST_REQUIRE(s1_loaded);
        BOOST_REQUIRE(s1_loaded->version() == s1->version());
        auto s1_later = local_schema_registry().get_or_null(s1->version());
        BOOST_REQUIRE(s1_later);

        auto s2_loaded = local_schema_registry().get_or_load(s2->version(), [s2] (table_schema_version) {
            return later().then([s2] { return frozen_schema_and_views(frozen_schema(s2), { }); });
        }).get0();

        BOOST_REQUIRE(s2_loaded);
        BOOST_REQUIRE(s2_loaded->version() == s2->version());
        auto s2_later = local_schema_registry().get_or_null(s2_loaded->version());
        BOOST_REQUIRE(s2_later);
    });
}

SEASTAR_TEST_CASE(test_async_loading_with_views) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s1 = random_schema();
        auto v1 = random_view(s1);
        auto s2 = random_schema();
        auto v2 = random_view(s2);

        auto s1_loaded = local_schema_registry().get_or_load(s1->version(), [s1, v1] (table_schema_version) {
            return make_ready_future<frozen_schema_and_views>(frozen_schema_and_views(frozen_schema(s1), { frozen_schema(v1) }));
        }).get0();

        BOOST_REQUIRE(s1_loaded->version() == s1->version());
        auto s1_later = local_schema_registry().get_or_null(s1->version());
        BOOST_REQUIRE(s1_later);

        BOOST_REQUIRE_EQUAL(s1_loaded->views().size(), 1);
        auto v1_from_s1 = s1_loaded->views()[0];
        BOOST_REQUIRE(v1->version() == v1_from_s1->version());
        auto v1_from_registry = local_schema_registry().get_or_null(v1->version());
        BOOST_REQUIRE(v1_from_registry);

        auto s2_loaded = local_schema_registry().get_or_load(s2->version(), [s2, v2] (table_schema_version) {
            return later().then([s2, v2] { return frozen_schema_and_views(frozen_schema(s2), { frozen_schema(v2) }); });
        }).get0();

        BOOST_REQUIRE(s2_loaded->version() == s2->version());
        auto s2_later = local_schema_registry().get_or_null(s2->version());
        BOOST_REQUIRE(s2_later);

        BOOST_REQUIRE_EQUAL(s2_loaded->views().size(), 1);
        auto v2_from_s2 = s2_loaded->views()[0];
        BOOST_REQUIRE(v2->version() == v2_from_s2->version());
        auto v2_from_registry = local_schema_registry().get_or_null(v2->version());
        BOOST_REQUIRE(v2_from_registry);
    });
}

SEASTAR_TEST_CASE(test_schema_is_synced_when_syncer_doesnt_defer) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = random_schema();
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) { return frozen_schema(s); });
        BOOST_REQUIRE(!s->is_synced());
        s->registry_entry()->maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_TEST_CASE(test_schema_is_synced_when_syncer_defers) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = random_schema();
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) { return frozen_schema(s); });
        BOOST_REQUIRE(!s->is_synced());
        s->registry_entry()->maybe_sync([] { return later(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_TEST_CASE(test_failed_sync_can_be_retried) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = random_schema();
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) { return frozen_schema(s); });
        BOOST_REQUIRE(!s->is_synced());

        promise<> fail_sync;

        auto f1 = s->registry_entry()->maybe_sync([&fail_sync] () mutable {
            return fail_sync.get_future().then([] {
                throw std::runtime_error("sync failed");
            });
        });

        // concurrent maybe_sync should attach the the current one
        auto f2 = s->registry_entry()->maybe_sync([] { return make_ready_future<>(); });

        fail_sync.set_value();

        try {
            f1.get();
            BOOST_FAIL("Should have failed");
        } catch (...) {
            // expected
        }

        try {
            f2.get();
            BOOST_FAIL("Should have failed");
        } catch (...) {
            // expected
        }

        BOOST_REQUIRE(!s->is_synced());

        s->registry_entry()->maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}
