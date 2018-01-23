/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "auth/default_authorizer.hh"

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

#include <chrono>
#include <random>

#include <boost/algorithm/string/join.hpp>
#include <boost/range.hpp>
#include <seastar/core/reactor.hh>

#include "auth/authenticated_user.hh"
#include "auth/common.hh"
#include "auth/permission.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"

namespace auth {

const sstring& default_authorizer_name() {
    static const sstring name = meta::AUTH_PACKAGE_NAME + "CassandraAuthorizer";
    return name;
}

static const sstring ROLE_NAME = "role";
static const sstring RESOURCE_NAME = "resource";
static const sstring PERMISSIONS_NAME = "permissions";
static const sstring PERMISSIONS_CF = "role_permissions";

static logging::logger alogger("default_authorizer");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
        authorizer,
        default_authorizer,
        cql3::query_processor&,
        ::service::migration_manager&> password_auth_reg("org.apache.cassandra.auth.CassandraAuthorizer");

default_authorizer::default_authorizer(cql3::query_processor& qp, ::service::migration_manager& mm)
        : _qp(qp)
        , _migration_manager(mm) {
}

default_authorizer::~default_authorizer() {
}

future<> default_authorizer::start() {
    static const sstring create_table = sprint(
            "CREATE TABLE %s.%s ("
            "%s text,"
            "%s text,"
            "%s set<text>,"
            "PRIMARY KEY(%s, %s)"
            ") WITH gc_grace_seconds=%d",
            meta::AUTH_KS,
            PERMISSIONS_CF,
            ROLE_NAME,
            RESOURCE_NAME,
            PERMISSIONS_NAME,
            ROLE_NAME,
            RESOURCE_NAME,
            90 * 24 * 60 * 60); // 3 months.

    return once_among_shards([this] {
        return create_metadata_table_if_missing(
                PERMISSIONS_CF,
                _qp,
                create_table,
                _migration_manager);
    });
}

future<> default_authorizer::stop() {
    return make_ready_future<>();
}

future<permission_set> default_authorizer::authorize_role_directly(
        stdx::string_view role_name,
        const resource& r,
        const service& ser) const {
    return ser.has_superuser(role_name).then([this, role_name, &r](bool has_superuser) {
        if (has_superuser) {
            return make_ready_future<permission_set>(r.applicable_permissions());
        }

        static const sstring query = sprint(
                "SELECT %s FROM %s.%s WHERE %s = ? AND %s = ?",
                PERMISSIONS_NAME,
                meta::AUTH_KS,
                PERMISSIONS_CF,
                ROLE_NAME,
                RESOURCE_NAME);

        return _qp.process(
                query,
                db::consistency_level::LOCAL_ONE,
                {sstring(role_name), r.name()}).then([](::shared_ptr<cql3::untyped_result_set> results) {
            if (results->empty()) {
                return permissions::NONE;
            }

            return permissions::from_strings(results->one().get_set<sstring>(PERMISSIONS_NAME));
        });
    });
}

future<permission_set> default_authorizer::authorize(
        stdx::string_view role_name,
        const resource& r,
        service& ser) const {
    return do_with(permission_set(), [this, &ser, role_name, &r](auto& ps) {
        return ser.get_roles(role_name).then([this, &ser, &ps, &r](std::unordered_set<sstring> all_roles) {
            return do_with(std::move(all_roles), [this, &ser, &ps, &r](const auto& all_roles) {
                return parallel_for_each(all_roles, [this, &ser, &ps, &r](stdx::string_view role_name) {
                    return this->authorize_role_directly(role_name, r, ser).then([&ps](permission_set rp) {
                        ps = permission_set::from_mask(ps.mask() | rp.mask());
                    });
                });
            });
        }).then([&ps] {
            return ps;
        });
    });
}

future<>
default_authorizer::modify(
        stdx::string_view role_name,
        permission_set set,
        const resource& resource,
        stdx::string_view op) {
    return do_with(
            sprint(
                    "UPDATE %s.%s SET %s = %s %s ? WHERE %s = ? AND %s = ?",
                    meta::AUTH_KS,
                    PERMISSIONS_CF,
                    PERMISSIONS_NAME,
                    PERMISSIONS_NAME,
                    op,
                    ROLE_NAME,
                    RESOURCE_NAME),
            [this, &role_name, set, &resource](const auto& query) {
        return _qp.process(
                query,
                db::consistency_level::ONE,
                {permissions::to_strings(set), sstring(role_name), resource.name()}).discard_result();
    });
}


future<> default_authorizer::grant(stdx::string_view role_name, permission_set set, const resource& resource) {
    return modify(role_name, std::move(set), resource, "+");
}

future<> default_authorizer::revoke(stdx::string_view role_name, permission_set set, const resource& resource) {
    return modify(role_name, std::move(set), resource, "-");
}

future<std::vector<permission_details>> default_authorizer::list(
        permission_set set,
        const std::optional<resource>& resource,
        const std::optional<stdx::string_view>& role_name,
        service& ser) const {
    sstring query = sprint(
            "SELECT %s, %s, %s FROM %s.%s",
            ROLE_NAME,
            RESOURCE_NAME,
            PERMISSIONS_NAME,
            meta::AUTH_KS,
            PERMISSIONS_CF);

    // Oh, look, it is a case where it does not pay off to have
    // parameters to process in an initializer list.
    future<::shared_ptr<cql3::untyped_result_set>> f = make_ready_future<::shared_ptr<cql3::untyped_result_set>>();

    if (role_name) {
        f = ser.get_roles(*role_name).then([this, &resource, query, &f](
                std::unordered_set<sstring> all_roles) mutable {
            if (resource) {
                query += sprint(" WHERE %s IN ? AND %s = ?", ROLE_NAME, RESOURCE_NAME);

                return do_with(
                        std::move(query),
                        std::move(all_roles),
                        [this, &resource](const auto& query, const auto& all_roles) {
                    return _qp.process(query, db::consistency_level::ONE, {all_roles, resource->name()});
                });
            }

            query += sprint(" WHERE %s IN ?", ROLE_NAME);

            return do_with(
                    std::move(query),
                    std::move(all_roles),
                    [this](const auto& query, const auto& all_roles) {
                return _qp.process(query, db::consistency_level::ONE, {all_roles});
            });
        });
    } else if (resource) {
        query += sprint(" WHERE %s = ? ALLOW FILTERING", RESOURCE_NAME);

        f = do_with(std::move(query), [this, &resource](const auto& query) {
            return _qp.process(query, db::consistency_level::ONE, {resource->name()});
        });
    } else {
        f = do_with(std::move(query), [this, &resource](const auto& query) {
            return _qp.process(query, db::consistency_level::ONE, {});
        });
    }

    return f.then([set](::shared_ptr<cql3::untyped_result_set> res) {
        std::vector<permission_details> result;

        for (auto& row : *res) {
            if (row.has(PERMISSIONS_NAME)) {
                auto username = row.get_as<sstring>(ROLE_NAME);
                auto resource = parse_resource(row.get_as<sstring>(RESOURCE_NAME));
                auto ps = permissions::from_strings(row.get_set<sstring>(PERMISSIONS_NAME));
                ps = permission_set::from_mask(ps.mask() & set.mask());

                result.emplace_back(permission_details {username, resource, ps});
            }
        }
        return make_ready_future<std::vector<permission_details>>(std::move(result));
    });
}

future<> default_authorizer::revoke_all(stdx::string_view role_name) {
    static const sstring query = sprint(
            "DELETE FROM %s.%s WHERE %s = ?",
            meta::AUTH_KS,
            PERMISSIONS_CF,
            ROLE_NAME);

    return _qp.process(
            query,
            db::consistency_level::ONE,
            {sstring(role_name)}).discard_result().handle_exception([role_name](auto ep) {
        try {
            std::rethrow_exception(ep);
        } catch (exceptions::request_execution_exception& e) {
            alogger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}", role_name, e);
        }
    });
}

future<> default_authorizer::revoke_all(const resource& resource) {
    static const sstring query = sprint(
            "SELECT %s FROM %s.%s WHERE %s = ? ALLOW FILTERING",
            ROLE_NAME,
            meta::AUTH_KS,
            PERMISSIONS_CF,
            RESOURCE_NAME);

    return _qp.process(
            query,
            db::consistency_level::LOCAL_ONE,
            {resource.name()}).then_wrapped([this, resource](future<::shared_ptr<cql3::untyped_result_set>> f) {
        try {
            auto res = f.get0();
            return parallel_for_each(
                    res->begin(),
                    res->end(),
                    [this, res, resource](const cql3::untyped_result_set::row& r) {
                static const sstring query = sprint(
                        "DELETE FROM %s.%s WHERE %s = ? AND %s = ?",
                        meta::AUTH_KS,
                        PERMISSIONS_CF,
                        ROLE_NAME,
                        RESOURCE_NAME);

                return _qp.process(
                        query,
                        db::consistency_level::LOCAL_ONE,
                        {r.get_as<sstring>(ROLE_NAME), resource.name()}).discard_result().handle_exception(
                                [resource](auto ep) {
                    try {
                        std::rethrow_exception(ep);
                    } catch (exceptions::request_execution_exception& e) {
                        alogger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", resource, e);
                    }

                });
            });
        } catch (exceptions::request_execution_exception& e) {
            alogger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", resource, e);
            return make_ready_future();
        }
    });
}

const resource_set& default_authorizer::protected_resources() const {
    static const resource_set resources({ make_data_resource(meta::AUTH_KS, PERMISSIONS_CF) });
    return resources;
}

}
