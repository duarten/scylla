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


#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_access_and_schema) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int, c ascii, v bigint, primary key (p, c));").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where v is not null and p is not null and c is not null "
                                 "primary key (v, p, c)").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into vcf (p, c, v) values (1, 'foo', 1);").then_wrapped([] (auto f) {
                assert_that_failed(f);
            });
        }).then([&e] {
            return e.execute_cql("alter table vcf add foo text;").then_wrapped([] (auto f) {
               assert_that_failed(f);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_reuse_name) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int primary key, v int);").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where v is not null and p is not null primary key (v, p)").discard_result();
        }).then([&e] {
            return e.execute_cql("drop materialized view vcf").discard_result();
        }).then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                  "where v is not null and p is not null "
                                  "primary key (v, p)").discard_result();
        });
    });
}

SEASTAR_TEST_CASE(test_drop_table_with_mv) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int PRIMARY KEY, v int);").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where v is not null and p is not null "
                                 "primary key (v, p)").discard_result();
        }).then([&e] {
            return e.execute_cql("drop table vcf").then_wrapped([] (auto f) {
                assert_that_failed(f);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_drop_table_with_active_mv) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int primary key, v int);").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where v is not null and p is not null "
                                 "primary key (v, p)").discard_result();
        }).then([&e] {
            return e.execute_cql("drop table cf").then_wrapped([] (auto f) {
                assert_that_failed(f);
            });
        }).then([&e] {
            return e.execute_cql("drop materialized view vcf").discard_result();
        }).then([&e] {
            return e.execute_cql("drop table cf").discard_result();
        });
    });
}

SEASTAR_TEST_CASE(test_alter_table) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int, c text, primary key (p, c));").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where p is not null and c is not null "
                                 "primary key (p, c)").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table cf alter c type blob").discard_result();
        });
    });
}

SEASTAR_TEST_CASE(test_alter_reversed_type_base_table) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int, c text, primary key (p, c)) with clustering order by (c desc);").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where p is not null and c is not null "
                                 "primary key (p, c) with clustering order by (c asc)").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table cf alter c type blob").discard_result();
        });
    });
}

SEASTAR_TEST_CASE(test_alter_reversed_type_view_table) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int, c text, primary key (p, c));").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where p is not null and c is not null "
                                 "primary key (p, c) with clustering order by (c desc)").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table cf alter c type blob").discard_result();
        });
    });
}

SEASTAR_TEST_CASE(test_alter_compatible_type) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int, c text, primary key (p));").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where p is not null and c is not null "
                                 "primary key (p, c) with clustering order by (c desc)").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table cf alter c type blob").discard_result();
        });
    });
}

SEASTAR_TEST_CASE(test_alter_incompatible_type) {
    return do_with_cql_env([] (auto& e) {
        return e.execute_cql("create table cf (p int, c int, primary key (p));").discard_result().then([&e] {
            return e.execute_cql("create materialized view vcf as select * from cf "
                                 "where p is not null and c is not null "
                                 "primary key (p, c) with clustering order by (c desc)").discard_result();
        }).then([&e] {
            return e.execute_cql("alter table cf alter c type blob").then_wrapped([](auto f) {
                assert_that_failed(f);
            });
        });
    });
}

