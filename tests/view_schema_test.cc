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
#include <boost/range/adaptor/map.hpp>

#include "database.hh"

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_case_sensitivity) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (theKey int, theClustering int, theValue int, primary key (theKey, theClustering));").get();
        e.execute_cql("create materialized view mv_test as select * from cf "
                       "where theKey is not null and theClustering is not null and theValue is not null "
                       "primary key (theKey,theClustering)").get();
        e.execute_cql("create materialized view mv_test2 as select theKey, theClustering, theValue from cf "
                       "where theKey is not null and theClustering is not null and theValue is not null "
                       "primary key (theKey,theClustering)").get();
        e.execute_cql("insert into cf (theKey, theClustering, theValue) values (0 ,0, 0);").get();

        for (auto view : {"mv_test", "mv_test2"}) {
            auto msg = e.execute_cql(sprint("select theKey, theClustering, theValue from %s ", view)).get0();
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(0)},
                    {int32_type->decompose(0)},
                });
        }

        e.execute_cql("alter table cf rename theClustering to Col;").get();

        for (auto view : {"mv_test", "mv_test2"}) {
            auto msg = e.execute_cql(sprint("select theKey, Col, theValue from %s ", view)).get0();
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(0)},
                    {int32_type->decompose(0)},
                });
        }
    });
}

SEASTAR_TEST_CASE(test_access_and_schema) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c ascii, v bigint, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                       "where v is not null and p is not null and c is not null "
                       "primary key (v, p, c)").get();
        e.execute_cql("insert into cf (p, c, v) values (0, 'foo', 1);").get();
        assert_that_failed(e.execute_cql("insert into vcf (p, c, v) values (1, 'foo', 1);"));
        assert_that_failed(e.execute_cql("alter table vcf add foo text;"));
        assert_that_failed(e.execute_cql("alter table vcf with compaction = { 'class' : 'LeveledCompactionStrategy' };"));
        e.execute_cql("alter materialized view vcf with compaction = { 'class' : 'LeveledCompactionStrategy' };").get();
        e.execute_cql("alter table cf add foo text;").get();
        e.execute_cql("insert into cf (p, c, v, foo) values (0, 'foo', 1, 'bar');").get();
        auto msg = e.execute_cql("select foo from vcf").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({
                {utf8_type->decompose(sstring("bar"))},
            });
        e.execute_cql("alter table cf rename c to bar;").get();
        msg = e.execute_cql("select bar from vcf").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({
                {utf8_type->decompose(sstring("foo"))},
            });
    });
}

SEASTAR_TEST_CASE(test_two_tables_one_view) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table dummy_table (p int, v int, primary key (p));").get();
        e.execute_cql("create table real_base (k int, v int, primary key (k));").get();
        e.execute_cql("create materialized view mv as select * from real_base "
                       "where k is not null and v is not null primary key (v, k)").get();
        e.execute_cql("create materialized view mv2 as select * from dummy_table "
                       "where p is not null and v is not null primary key (v, p)").get();

        e.execute_cql("insert into real_base (k, v) values (0, 0);").get();
        auto msg = e.execute_cql("select k, v from real_base where k = 0").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        msg = e.execute_cql("select k, v from mv where v = 0").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)} });

        // TODO: complete when we support updates
    });
}

SEASTAR_TEST_CASE(test_reuse_name) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int primary key, v int);").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null primary key (v, p)").get();
        e.execute_cql("drop materialized view vcf").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null "
                      "primary key (v, p)").get();
    });
}

SEASTAR_TEST_CASE(test_all_types) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TYPE myType (a int, b uuid, c set<text>)").get();
        e.execute_cql("CREATE TABLE cf ("
                    "k int PRIMARY KEY, "
                    "asciival ascii, "
                    "bigintval bigint, "
                    "blobval blob, "
                    "booleanval boolean, "
                    "dateval date, "
                    "decimalval decimal, "
                    "doubleval double, "
                    "floatval float, "
                    "inetval inet, "
                    "intval int, "
                    "textval text, "
                    "timeval time, "
                    "timestampval timestamp, "
                    "timeuuidval timeuuid, "
                    "uuidval uuid,"
                    "varcharval varchar, "
                    "varintval varint, "
                    "listval list<int>, "
                    "frozenlistval frozen<list<int>>, "
                    "setval set<uuid>, "
                    "frozensetval frozen<set<uuid>>, "
                    "mapval map<ascii, int>,"
                    "frozenmapval frozen<map<ascii, int>>,"
                    "tupleval frozen<tuple<int, ascii, uuid>>,"
                    "udtval frozen<myType>)").get();
        auto s = e.local_db().find_schema(sstring("ks"), sstring("cf"));
        BOOST_REQUIRE(s);
        for (auto* col : s->all_columns() | boost::adaptors::map_values) {
            auto f = e.execute_cql(sprint("create materialized view mv%i as select * from cf "
                                          "where %s is not null and k is not null primary key (%s, k)",
                                          col->id, col->name_as_text(), col->name_as_text()));
            if (col->type->is_multi_cell() || col->is_partition_key()) {
                assert_that_failed(f);
            } else {
                f.get();
            }
        }

        // TODO: complete when we support updates
    });
}

SEASTAR_TEST_CASE(test_drop_table_with_mv) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int PRIMARY KEY, v int);").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null "
                      "primary key (v, p)").get();
        assert_that_failed(e.execute_cql("drop table vcf"));
    });
}

SEASTAR_TEST_CASE(test_drop_table_with_active_mv) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int primary key, v int);").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null "
                      "primary key (v, p)").get();
        assert_that_failed(e.execute_cql("drop table cf"));
        e.execute_cql("drop materialized view vcf").get();
        e.execute_cql("drop table cf").get();
    });
}

SEASTAR_TEST_CASE(test_alter_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c text, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();
        e.execute_cql("alter table cf alter c type blob").get();
    });
}

SEASTAR_TEST_CASE(test_alter_reversed_type_base_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c text, primary key (p, c)) with clustering order by (c desc);").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c) with clustering order by (c asc)").get();
        e.execute_cql("alter table cf alter c type blob").get();
    });
}

SEASTAR_TEST_CASE(test_alter_reversed_type_view_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c text, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c) with clustering order by (c desc)").get();
        e.execute_cql("alter table cf alter c type blob").get();
    });
}

SEASTAR_TEST_CASE(test_alter_compatible_type) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c text, primary key (p));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c) with clustering order by (c desc)").get();
        e.execute_cql("alter table cf alter c type blob").get();
    });
}

SEASTAR_TEST_CASE(test_alter_incompatible_type) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, primary key (p));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c) with clustering order by (c desc)").get();
        assert_that_failed(e.execute_cql("alter table cf alter c type blob"));
    });
}

SEASTAR_TEST_CASE(test_drop_non_existing) {
    return do_with_cql_env_thread([] (auto& e) {
        assert_that_failed(e.execute_cql("drop materialized view view_doees_not_exist;"));
        assert_that_failed(e.execute_cql("drop materialized view keyspace_does_not_exist.view_doees_not_exist;"));
        e.execute_cql("drop materialized view if exists view_doees_not_exist;").get();
        e.execute_cql("drop materialized view if exists keyspace_does_not_exist.view_doees_not_exist;").get();
    });
}

SEASTAR_TEST_CASE(test_create_mv_with_unrestricted_pk_parts) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c ascii, v bigint, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select p from cf "
                       "where v is not null and p is not null and c is not null "
                       "primary key (v, p, c)").get();
        e.execute_cql("insert into cf (p, c, v) values (0, 'foo', 1);").get();
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {long_type->decompose(1L)}, {int32_type->decompose(0)}, {utf8_type->decompose(sstring("foo"))} });
    });
}

