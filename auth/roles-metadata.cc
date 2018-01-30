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

#include "auth/roles-metadata.hh"

#include <seastar/core/print.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include "auth/common.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"

namespace auth {

future<bool> default_role_row_satisfies(
        cql3::query_processor& qp,
        std::function<bool(const cql3::untyped_result_set_row&)> p) {
    static const sstring query = sprint(
            "SELECT * FROM %s WHERE %s = ?",
            meta::roles_table::qualified_name(),
            meta::roles_table::role_col_name);

    return do_with(std::move(p), [&qp](const auto& p) {
        return qp.process(
                query,
                db::consistency_level::ONE,
                {meta::DEFAULT_SUPERUSER_NAME},
                true).then([&qp, &p](::shared_ptr<cql3::untyped_result_set> results) {
            if (results->empty()) {
                return qp.process(
                        query,
                        db::consistency_level::QUORUM,
                        {meta::DEFAULT_SUPERUSER_NAME},
                        true).then([&qp, &p](::shared_ptr<cql3::untyped_result_set> results) {
                    if (results->empty()) {
                        return make_ready_future<bool>(false);
                    }

                    return make_ready_future<bool>(p(results->one()));
                });
            }

            return make_ready_future<bool>(p(results->one()));
        });
    });
}

future<bool> any_nondefault_role_row_satisfies(
        cql3::query_processor& qp,
        std::function<bool(const cql3::untyped_result_set_row&)> p) {
    static const sstring query = sprint("SELECT * FROM %s", meta::roles_table::qualified_name());

    return do_with(std::move(p), [&qp](const auto& p) {
        return qp.process(
                query,
                db::consistency_level::QUORUM).then([&p](::shared_ptr<cql3::untyped_result_set> results) {
            if (results->empty()) {
                return make_ready_future<bool>(false);
            }

            static const sstring col_name = sstring(meta::roles_table::role_col_name);

            for (const auto& row : *results) {
                if (row.get_as<sstring>(col_name) != meta::DEFAULT_SUPERUSER_NAME) {
                    return make_ready_future<bool>(p(row));
                }
            }

            return make_ready_future<bool>(false);
        });
    });
}

}
