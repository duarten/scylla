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

#include <experimental/string_view>
#include <memory>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "auth/authenticator.hh"
#include "auth/authorizer.hh"
#include "auth/permission.hh"
#include "auth/permissions_cache.hh"
#include "auth/role_manager.hh"
#include "seastarx.hh"
#include "stdx.hh"

namespace cql3 {
class query_processor;
}

namespace db {
class config;
}

namespace service {
class migration_manager;
class migration_listener;
}

namespace auth {

struct service_config final {
    static service_config from_db_config(const db::config&);

    sstring authorizer_java_name;
    sstring authenticator_java_name;
    sstring role_manager_java_name;
};

///
/// Due to poor (in this author's opinion) decisions of Apache Cassandra, certain choices of one role-manager,
/// authenticator, or authorizer imply restrictions on the rest.
///
/// This exception is thrown when an invalid combination of modules is selected, with a message explaining the
/// incompatibility.
///
class incompatible_module_combination : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

///
/// Central interface into access-control for the system.
///
/// Access control encompasses user/role management, authentication, and authorization. This class provides access to
/// the dynamically-loaded implementations of these modules (through the `underlying_*` member functions), but also
/// builds on their functionality with caching and abstractions for common operations.
///
class service final {
    permissions_cache_config _permissions_cache_config;
    std::unique_ptr<permissions_cache> _permissions_cache;

    cql3::query_processor& _qp;

    ::service::migration_manager& _migration_manager;

    std::unique_ptr<authorizer> _authorizer;

    std::unique_ptr<authenticator> _authenticator;

    std::unique_ptr<role_manager> _role_manager;

    // Only one of these should be registered, so we end up with some unused instances. Not the end of the world.
    std::unique_ptr<::service::migration_listener> _migration_listener;

public:
    service(
            permissions_cache_config,
            cql3::query_processor&,
            ::service::migration_manager&,
            std::unique_ptr<authorizer>,
            std::unique_ptr<authenticator>,
            std::unique_ptr<role_manager>);

    ///
    /// This constructor is intended to be used when the class is sharded via \ref seastar::sharded. In that case, the
    /// arguments must be copyable, which is why we delay construction with instance-construction instructions instead
    /// of the instances themselves.
    ///
    service(
            permissions_cache_config,
            cql3::query_processor&,
            ::service::migration_manager&,
            const service_config&);

    future<> start();

    future<> stop();

    future<permission_set> get_permissions(stdx::string_view role_name, const resource&) const;

    ///
    /// Query whether the named role has been granted a role that is a superuser.
    ///
    /// A role is always granted to itself. Therefore, a role that "is" a superuser also "has" superuser.
    ///
    /// \returns an exceptional future with \ref nonexistant_role if the role does not exist.
    ///
    future<bool> has_superuser(stdx::string_view role_name) const;

    ///
    /// Return the set of all roles granted to the given role, including itself and roles granted through other roles.
    ///
    /// \returns an exceptional future with \ref nonexistent_role if the role does not exist.
    future<std::unordered_set<sstring>> get_roles(stdx::string_view role_name) const;

    authenticator& underlying_authenticator() {
        return *_authenticator;
    }

    const authenticator& underlying_authenticator() const {
        return *_authenticator;
    }

    authorizer& underlying_authorizer() {
        return *_authorizer;
    }

    const authorizer& underlying_authorizer() const {
        return *_authorizer;
    }

    role_manager& underlying_role_manager() {
        return *_role_manager;
    }

    const role_manager& underlying_role_manager() const {
        return *_role_manager;
    }

private:
    future<bool> has_existing_legacy_users() const;

    future<> create_keyspace_if_missing() const;
};

future<bool> has_superuser(const service&, const authenticated_user&);

future<std::unordered_set<sstring>> get_roles(const service&, const authenticated_user&);

///
/// Access-control is "enforcing" when either the authenticator or the authorizer are not their "allow-all" variants.
///
/// Put differently, when access control is not enforcing, all operations on resources will be allowed and users do not
/// need to authenticate themselves.
///
bool is_enforcing(const service&);

///
/// Create a role with optional authentication information.
///
/// \returns an exceptional future with \ref role_already_exists if the user or role exists.
///
/// \returns an exceptional future with \ref unsupported_authentication_option if an unsupported option is included.
///
future<> create_role(
        service&,
        stdx::string_view name,
        const role_config&,
        const authentication_options&);

///
/// Alter an existing role and its authentication information.
///
/// \returns an exceptional future with \ref nonexistant_role if the named role does not exist.
///
/// \returns an exceptional future with \ref unsupported_authentication_option if an unsupported option is included.
///
future<> alter_role(
        service&,
        stdx::string_view name,
        const role_config_update&,
        const authentication_options&);

///
/// Drop a role from the system, including all permissions and authentication information.
///
/// \returns an exceptional future with \ref nonexistant_role if the named role does not exist.
///
future<> drop_role(service&, stdx::string_view name);

///
/// Check if `grantee` has been granted the named role.
///
/// \returns an exceptional future with \ref nonexistent_role if `grantee` or `name` do not exist.
///
future<bool> has_role(const service&, stdx::string_view grantee, stdx::string_view name);
///
/// Check if the authenticated user has been granted the named role.
///
/// \returns an exceptional future with \ref nonexistent_role if the user or `name` do not exist.
///
future<bool> has_role(const service&, const authenticated_user&, stdx::string_view name);

}
