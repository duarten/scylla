/*
 * Copyright (C) 2014 ScyllaDB
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

// Some thrift headers include other files from within namespaces,
// which is totally broken.  Include those files here to avoid
// breakage:
#include <sys/param.h>
// end thrift workaround
#include "Cassandra.h"
#include "core/distributed.hh"
#include "database.hh"
#include "core/sstring.hh"
#include "core/print.hh"
#include "frozen_mutation.hh"
#include "utils/UUID_gen.hh"
#include <thrift/protocol/TBinaryProtocol.h>
#include <boost/move/iterator.hpp>
#include "db/marshal/type_parser.hh"
#include "service/migration_manager.hh"
#include "utils/class_registrator.hh"
#include "noexcept_traits.hh"
#include "schema_registry.hh"
#include "utils/exceptions.hh"
#include "thrift_validation.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include <boost/range/adaptor/uniqued.hpp>
#include "query-result-reader.hh"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::async;

using namespace  ::org::apache::cassandra;

class unimplemented_exception : public std::exception {
public:
    virtual const char* what() const throw () override { return "sorry, not implemented"; }
};

void pass_unimplemented(const tcxx::function<void(::apache::thrift::TDelayedException* _throw)>& exn_cob) {
    exn_cob(::apache::thrift::TDelayedException::delayException(unimplemented_exception()));
}

class delayed_exception_wrapper : public ::apache::thrift::TDelayedException {
    std::exception_ptr _ex;
public:
    delayed_exception_wrapper(std::exception_ptr ex) : _ex(std::move(ex)) {}
    virtual void throw_it() override {
        // Thrift auto-wraps unexpected exceptions (those not derived from TException)
        // with a TException, but with a fairly bad what().  So detect this, and
        // provide our own TException with a better what().
        try {
            std::rethrow_exception(std::move(_ex));
        } catch (const ::apache::thrift::TException&) {
            // It's an expected exception, so assume the message
            // is fine.  Also, we don't want to change its type.
            throw;
        } catch (no_such_class& nc) {
            throw make_exception<InvalidRequestException>(nc.what());
        } catch (std::exception& e) {
            // Unexpected exception, wrap it
            throw ::apache::thrift::TException(std::string("Internal server error: ") + e.what());
        } catch (...) {
            // Unexpected exception, wrap it, unfortunately without any info
            throw ::apache::thrift::TException("Internal server error");
        }
    }
};

template <typename Func, typename T>
void
with_cob(tcxx::function<void (const T& ret)>&& cob,
        tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        Func&& func) {
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<noexcept_movable_t<T>>::apply([func = std::forward<Func>(func)] {
        return noexcept_movable<T>::wrap(func());
    }).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (auto&& f) {
        try {
            cob(noexcept_movable<T>::unwrap(f.get0()));
        } catch (...) {
            delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
        }
    });
}

template <typename Func, typename T>
void
with_cob_dereference(tcxx::function<void (const T& ret)>&& cob,
        tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        Func&& func) {
    using ptr_type = foreign_ptr<lw_shared_ptr<T>>;
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<ptr_type>::apply(func).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<ptr_type> f) {
        try {
            cob(*f.get0());
        } catch (...) {
            delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
        }
    });
}

template <typename Func>
void
with_cob(tcxx::function<void ()>&& cob,
        tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        Func&& func) {
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<void>::apply(func).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<> f) {
        try {
            f.get();
            cob();
        } catch (...) {
            delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
        }
    });
}

std::string bytes_to_string(bytes_view v) {
    return { reinterpret_cast<const char*>(v.begin()), v.size() };
}

class thrift_handler : public CassandraCobSvIf {
    distributed<database>& _db;
    sstring _ks_name;
    sstring _cql_version;
public:
    explicit thrift_handler(distributed<database>& db) : _db(db) {}
    void login(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const AuthenticationRequest& auth_request) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void set_keyspace(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (!_db.local().has_keyspace(keyspace)) {
                throw make_exception<InvalidRequestException>("keyspace %s does not exist", keyspace);
            } else {
                _ks_name = keyspace;
            }
        });
    }

    void get(tcxx::function<void(ColumnOrSuperColumn const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const ConsistencyLevel::type consistency_level) {
        return get_slice([cob = std::move(cob), &column_path](auto&& results) {
            if (results.empty()) {
                throw NotFoundException();
            }
            return cob(std::move(results.front()));
        }, exn_cob, key, column_path_to_column_parent(column_path), column_path_to_slice_predicate(column_path), std::move(consistency_level));
    }

    void get_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        return multiget_slice([cob = std::move(cob)](auto&& results) {
            if (!results.empty()) {
                return cob(std::move(results.begin()->second));
            }
            return cob({ });
        }, exn_cob, {key}, column_parent, predicate, consistency_level);
    }

    void get_count(tcxx::function<void(int32_t const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        return multiget_count([cob = std::move(cob)](auto&& results) {
            if (!results.empty()) {
                return cob(results.begin()->second);
            }
            return cob(0);
        }, exn_cob, {key}, column_parent, predicate, consistency_level);
    }

    void multiget_slice(tcxx::function<void(std::map<std::string, std::vector<ColumnOrSuperColumn> >  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (!column_parent.super_column.empty()) {
                throw unimplemented_exception();
            }
            auto schema = lookup_schema(_db.local(), _ks_name, column_parent.column_family);
            auto cmd = slice_pred_to_read_cmd(*schema, predicate);
            return service::get_local_storage_proxy().query(
                    schema,
                    cmd,
                    make_partition_ranges(*schema, keys),
                    cl_from_thrift(consistency_level)).then([schema, cmd](auto result) {
                return query::result_view::do_with(*result, [schema, cmd](query::result_view v) {
                    column_aggregator aggregator(*schema, cmd->slice);
                    v.consume(cmd->slice, aggregator);
                    return aggregator.release();
                });
            });
        });
    }

    void multiget_count(tcxx::function<void(std::map<std::string, int32_t>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (!column_parent.super_column.empty()) {
                throw unimplemented_exception();
            }
            auto schema = lookup_schema(_db.local(), _ks_name, column_parent.column_family);
            auto cmd = slice_pred_to_read_cmd(*schema, predicate);
            return service::get_local_storage_proxy().query(
                    schema,
                    cmd,
                    make_partition_ranges(*schema, keys),
                    cl_from_thrift(consistency_level)).then([schema, cmd](auto&& result) {
                return query::result_view::do_with(*result, [schema, cmd](query::result_view v) {
                    column_counter counter(*schema, cmd->slice);
                    v.consume(cmd->slice, counter);
                    return counter.release();
                });
            });
        });
    }

    /**
     * In origin, empty partitions are returned as part of the KeySlice, for which the key will be filled
     * in but the columns vector will be empty. Since in our case we don't return empty partitions, we
     * don't know which partition keys in the specified range we should return back to the client. So for
     * now our behavior differs from Origin.
     */
    void get_range_slices(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const ColumnParent& column_parent, const SlicePredicate& predicate, const KeyRange& range, const ConsistencyLevel::type consistency_level) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (!column_parent.super_column.empty()) {
                throw unimplemented_exception();
            }
            auto schema = lookup_schema(_db.local(), _ks_name, column_parent.column_family);
            auto&& prange = make_partition_range(*schema, range);
            auto cmd = slice_pred_to_read_cmd(*schema, predicate);
            // KeyRange::count is the number of thrift rows to return, while
            // SlicePredicte::slice_range::count limits the number of thrift colums.
            if (is_dynamic(*schema)) {
                // For dynamic CFs we must limit the number of partitions returned.
                cmd->partition_limit = range.count;
            } else {
                // For static CFs each thrift row maps to a CQL row.
                cmd->row_limit = range.count;
            }
            return service::get_local_storage_proxy().query(
                    schema,
                    cmd,
                    {std::move(prange)},
                    cl_from_thrift(consistency_level)).then([schema, cmd](auto result) {
                return query::result_view::do_with(*result, [schema, cmd](query::result_view v) {
                    return to_key_slices(*schema, cmd->slice, v);
                });
            });
        });
    }

    static lw_shared_ptr<query::read_command> make_paged_read_cmd(const schema& s, uint32_t remaining, const std::string* start_column) {
        auto opts = query_opts(s);
        std::vector<query::clustering_range> clustering_ranges;
        std::vector<column_id> regular_columns;
        uint32_t row_limit;
        uint32_t partition_limit;
        // KeyRange::count is the number of thrift columns to return (unlike get_range_slices).
        if (is_dynamic(s)) {
            // For dynamic CFs we must limit the number of rows returned. Since we don't know any actual partition key,
            // we can't use the partition_slice::specific_ranges. Instead, we ask for an initial partition to consume
            // the remainder of thrift columns (here, CQL rows), and potentially emit a second query to consume
            // the remainder of columns across all subsequent partitions.
            row_limit = remaining;
            partition_limit = query::max_partitions;
            if (start_column) {
                clustering_ranges.emplace_back(make_clustering_range(s, *start_column, std::string()));
            } else {
                clustering_ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
            }
            regular_columns.emplace_back(s.regular_begin()->id);
        } else {
            // For static CFs we must limit the number of columns returned. Like with dynamic CFs, we ask for
            // one partition to consume the remainder of columns in that first partition. Then, we ask for as
            // many full partitions as the range count allows us. Eventually, we'll make a third query to a new
            // partition for the remainder of columns to reach the specified count.
            auto start = start_column ? s.regular_lower_bound(to_bytes(*start_column)) : s.regular_begin();
            auto size = std::min(remaining, static_cast<uint32_t>(std::distance(start, s.regular_end())));
            row_limit = query::max_rows;
            partition_limit = remaining / size;
            clustering_ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
            add_columns(start, s.regular_end(), regular_columns, size, false);
        }
        auto slice = query::partition_slice(std::move(clustering_ranges), {}, std::move(regular_columns), opts,
                nullptr, cql_serialization_format::internal());
        auto cmd = make_lw_shared<query::read_command>(s.id(), s.version(), std::move(slice), row_limit);
        cmd->partition_limit = start_column ? 1 : partition_limit;
        return cmd;
    }

    static future<> do_get_paged_slice(
            schema_ptr schema,
            uint32_t count,
            const query::partition_range range,
            const std::string* start_column,
            db::consistency_level consistency_level,
            std::vector<KeySlice>& output) {
        auto cmd = make_paged_read_cmd(*schema, count, start_column);
        auto end = range.end();
        return service::get_local_storage_proxy().query(schema, cmd, {std::move(range)}, consistency_level).then([schema, cmd](auto result) {
            return query::result_view::do_with(*result, [schema, cmd](query::result_view v) {
                return to_key_slices(*schema, cmd->slice, v);
            });
        }).then([schema, cmd, count, consistency_level, end = std::move(end), &output](auto&& slices) {
            auto columns = std::accumulate(slices.begin(), slices.end(), 0u, [](auto&& acc, auto&& ks) {
                return acc + ks.columns.size();
            });
            std::move(slices.begin(), slices.end(), std::back_inserter(output));
            if (columns == 0 || columns >= count || (slices.size() < cmd->partition_limit && columns < cmd->row_limit)) {
                return make_ready_future();
            }
            auto start = dht::global_partitioner().decorate_key(*schema, key_from_thrift(*schema, to_bytes(slices.back().key)));
            auto new_range = query::partition_range(query::partition_range::bound(start, false), std::move(end));
            return do_get_paged_slice(schema, count - columns, std::move(new_range), nullptr, consistency_level, output);
        });
    }

    void get_paged_slice(tcxx::function<void(std::vector<KeySlice> const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family, const KeyRange& range, const std::string& start_column, const ConsistencyLevel::type consistency_level) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            return do_with(std::vector<KeySlice>(), [&](auto& output) {
                if (range.__isset.row_filter) {
                    throw make_exception<InvalidRequestException>("Cross-row paging is not supported along with index clauses");
                }
                if (range.count <= 0) {
                    throw make_exception<InvalidRequestException>("Count must be positive");
                }
                auto schema = lookup_schema(_db.local(), _ks_name, column_family);
                auto&& prange = make_partition_range(*schema, range);
                return do_get_paged_slice(std::move(schema), range.count, std::move(prange), &start_column,
                        cl_from_thrift(consistency_level), output).then([&output] {
                    return std::move(output);
                });
            });
        });
    }

    void get_indexed_slices(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const ColumnParent& column_parent, const IndexClause& index_clause, const SlicePredicate& column_predicate, const ConsistencyLevel::type consistency_level) {
        std::vector<KeySlice>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void insert(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const Column& column, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void add(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const CounterColumn& column, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void cas(tcxx::function<void(CASResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const std::string& column_family, const std::vector<Column> & expected, const std::vector<Column> & updates, const ConsistencyLevel::type serial_consistency_level, const ConsistencyLevel::type commit_consistency_level) {
        CASResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void remove(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const int64_t timestamp, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void remove_counter(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& path, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void batch_mutate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel::type consistency_level) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (_ks_name.empty()) {
                throw make_exception<InvalidRequestException>("keyspace not set");
            }
            // Would like to use move_iterator below, but Mutation is filled with some const stuff.
            return parallel_for_each(mutation_map.begin(), mutation_map.end(),
                    [this] (std::pair<std::string, std::map<std::string, std::vector<Mutation>>> key_cf) {
                bytes thrift_key = to_bytes(key_cf.first);
                std::map<std::string, std::vector<Mutation>>& cf_mutations_map = key_cf.second;
                return parallel_for_each(
                        boost::make_move_iterator(cf_mutations_map.begin()),
                        boost::make_move_iterator(cf_mutations_map.end()),
                        [this, thrift_key] (std::pair<std::string, std::vector<Mutation>> cf_mutations) {
                    sstring cf_name = cf_mutations.first;
                    const std::vector<Mutation>& mutations = cf_mutations.second;
                    auto schema = lookup_schema(_db.local(), _ks_name, cf_name);
                    mutation m_to_apply(key_from_thrift(*schema, thrift_key), schema);
                    auto empty_clustering_key = clustering_key::make_empty();
                    for (const Mutation& m : mutations) {
                        if (m.__isset.column_or_supercolumn) {
                            auto&& cosc = m.column_or_supercolumn;
                            if (cosc.__isset.column) {
                                auto&& col = cosc.column;
                                bytes cname = to_bytes(col.name);
                                auto def = schema->get_column_definition(cname);
                                if (!def) {
                                    throw make_exception<InvalidRequestException>("column %s not found", col.name);
                                }
                                if (def->kind != column_kind::regular_column) {
                                    throw make_exception<InvalidRequestException>("Column %s is not settable", col.name);
                                }
                                m_to_apply.set_clustered_cell(empty_clustering_key, *def,
                                    atomic_cell::make_live(col.timestamp, to_bytes(col.value), maybe_ttl(*schema, col)));
                            } else if (cosc.__isset.super_column) {
                                // FIXME: implement
                            } else if (cosc.__isset.counter_column) {
                                // FIXME: implement
                            } else if (cosc.__isset.counter_super_column) {
                                // FIXME: implement
                            } else {
                                throw make_exception<InvalidRequestException>("Empty ColumnOrSuperColumn");
                            }
                        } else if (m.__isset.deletion) {
                            // FIXME: implement
                            abort();
                        } else {
                            throw make_exception<InvalidRequestException>("Mutation must have either column or deletion");
                        }
                    }
                    auto shard = _db.local().shard_of(m_to_apply);
                    return _db.invoke_on(shard, [this, gs = global_schema_ptr(schema), cf_name, m = freeze(m_to_apply)] (database& db) {
                        return db.apply(gs, m);
                    });
                });
            });
        });
    }

    void atomic_batch_mutate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void truncate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfname) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void get_multi_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const MultiSliceRequest& request) {
        std::vector<ColumnOrSuperColumn>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_schema_versions(tcxx::function<void(std::map<std::string, std::vector<std::string> >  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        std::map<std::string, std::vector<std::string> >  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_keyspaces(tcxx::function<void(std::vector<KsDef>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            std::vector<KsDef>  ret;
            for (auto&& ks : _db.local().keyspaces()) {
                ret.emplace_back(get_keyspace_definition(ks.second));
            }
            return ret;
        });
    }

    void describe_cluster_name(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        cob("seastar");
    }

    void describe_version(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        cob("0.0.0");
    }

    void describe_ring(tcxx::function<void(std::vector<TokenRange>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        std::vector<TokenRange>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_local_ring(tcxx::function<void(std::vector<TokenRange>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        std::vector<TokenRange>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_token_map(tcxx::function<void(std::map<std::string, std::string>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        std::map<std::string, std::string>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_partitioner(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        return cob("dummy paritioner");
    }

    void describe_snitch(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        return cob("dummy snitch");
    }

    void describe_keyspace(tcxx::function<void(KsDef const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            try {
                auto& ks = _db.local().find_keyspace(keyspace);
                return get_keyspace_definition(ks);
            } catch (no_such_keyspace& nsk) {
                throw make_exception<InvalidRequestException>("keyspace %s does not exist", keyspace);
            }
        });
    }

    void describe_splits(tcxx::function<void(std::vector<std::string>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) {
        std::vector<std::string>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void trace_next_query(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        return cob("dummy trace");
    }

    void describe_splits_ex(tcxx::function<void(std::vector<CfSplit>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) {
        std::vector<CfSplit>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_add_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_drop_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_add_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            std::string schema_id = "schema-id";  // FIXME: make meaningful
            if (_db.local().has_keyspace(ks_def.name)) {
                InvalidRequestException ire;
                ire.why = sprint("Keyspace %s already exists", ks_def.name);
                throw ire;
            }

            std::vector<schema_ptr> cf_defs;
            cf_defs.reserve(ks_def.cf_defs.size());
            for (const CfDef& cf_def : ks_def.cf_defs) {
                std::vector<schema::column> partition_key;
                std::vector<schema::column> clustering_key;
                std::vector<schema::column> regular_columns;
                // FIXME: get this from comparator
                auto column_name_type = utf8_type;
                // FIXME: look at key_alias and key_validation_class first
                partition_key.push_back({"key", bytes_type});
                if (cf_def.column_metadata.empty()) {
                    // Dynamic CF
                    auto ck_types = get_types(cf_def.comparator_type);
                    for (uint32_t i = 0; i < ck_types.size(); ++i) {
                        clustering_key.push_back({"column" + (i + 1), std::move(ck_types[i])});
                    }
                    auto&& vtype = cf_def.__isset.default_validation_class
                                 ? db::marshal::type_parser::parse(sstring_view(cf_def.default_validation_class))
                                 : utf8_type;
                    regular_columns.push_back({"value", std::move(vtype)});
                } else {
                    // Static CF
                    for (const ColumnDef& col_def : cf_def.column_metadata) {
                        // FIXME: look at all fields, not just name
                        regular_columns.push_back({to_bytes(col_def.name), bytes_type});
                    }
                }
                auto id = utils::UUID_gen::get_time_UUID();
                auto s = make_lw_shared(schema(id, ks_def.name, cf_def.name,
                    std::move(partition_key), std::move(clustering_key), std::move(regular_columns),
                    std::vector<schema::column>(), column_name_type));
                cf_defs.push_back(s);
            }
            auto ksm = make_lw_shared<keyspace_metadata>(to_sstring(ks_def.name),
                to_sstring(ks_def.strategy_class),
                std::map<sstring, sstring>{ks_def.strategy_options.begin(), ks_def.strategy_options.end()},
                ks_def.durable_writes,
                std::move(cf_defs));
            return service::get_local_migration_manager().announce_new_keyspace(ksm, false).then([schema_id = std::move(schema_id)] {
                return make_ready_future<std::string>(std::move(schema_id));
            });
        });
    }

    void system_drop_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_update_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_update_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void execute_cql_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void execute_cql3_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression, const ConsistencyLevel::type consistency) {
        print("warning: ignoring query %s\n", query);
        cob({});
#if 0
        CqlResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
#endif
    }

    void prepare_cql_query(tcxx::function<void(CqlPreparedResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlPreparedResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void prepare_cql3_query(tcxx::function<void(CqlPreparedResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlPreparedResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void execute_prepared_cql_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t itemId, const std::vector<std::string> & values) {
        CqlResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void execute_prepared_cql3_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t itemId, const std::vector<std::string> & values, const ConsistencyLevel::type consistency) {
        CqlResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void set_cql_version(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& version) {
        _cql_version = version;
        cob();
    }

private:
    static sstring class_from_data_type(const data_type& dt) {
        static const std::unordered_map<sstring, sstring> types = {
            { "boolean", "BooleanType" },
            { "bytes", "BytesType" },
            { "double", "DoubleType" },
            { "int32", "Int32Type" },
            { "long", "LongType" },
            { "timestamp", "DateType" },
            { "timeuuid", "TimeUUIDType" },
            { "utf8", "UTF8Type" },
            { "uuid", "UUIDType" },
            // FIXME: missing types
        };
        auto it = types.find(dt->name());
        if (it == types.end()) {
            return sstring("<unknown> ") + dt->name();
        }
        return sstring("org.apache.cassandra.db.marshal.") + it->second;
    }
    template<allow_prefixes IsPrefixable>
    static sstring class_from_compound_type(const compound_type<IsPrefixable>& ct) {
        if (ct.is_singular()) {
            return class_from_data_type(ct.types().front());
        }
        sstring type = "org.apache.cassandra.db.marshal.CompositeType(";
        for (auto& dt : ct.types()) {
            type += class_from_data_type(dt);
            if (&dt != &*ct.types().rbegin()) {
                type += ",";
            }
        }
        type += ")";
        return type;
    }
    static std::vector<data_type> get_types(const std::string& thrift_type) {
        static const char composite_type[] = "CompositeType";
        std::vector<data_type> ret;
        auto t = sstring_view(thrift_type);
        auto composite_idx = t.find(composite_type);
        if (composite_idx == sstring_view::npos) {
            ret.emplace_back(db::marshal::type_parser::parse(t));
        } else {
            t.remove_prefix(composite_idx + sizeof(composite_type) - 1);
            auto types = db::marshal::type_parser(t).get_type_parameters(false);
            std::move(types.begin(), types.end(), std::back_inserter(ret));
        }
        return ret;
    }
    static KsDef get_keyspace_definition(const keyspace& ks) {
        auto&& meta = ks.metadata();
        KsDef def;
        def.__set_name(meta->name());
        def.__set_strategy_class(meta->strategy_name());
        std::map<std::string, std::string> options(
            meta->strategy_options().begin(),
            meta->strategy_options().end());
        def.__set_strategy_options(options);
        std::vector<CfDef> cfs;
        for (auto&& cf : meta->cf_meta_data()) {
            // FIXME: skip cql3 column families
            auto&& s = cf.second;
            CfDef cf_def;
            cf_def.__set_keyspace(s->ks_name());
            cf_def.__set_name(s->cf_name());
            cf_def.__set_key_validation_class(class_from_compound_type(*s->partition_key_type()));
            if (s->clustering_key_size()) {
                cf_def.__set_comparator_type(class_from_compound_type(*s->clustering_key_type()));
            } else {
                cf_def.__set_comparator_type(class_from_data_type(s->regular_column_name_type()));
            }
            cf_def.__set_comment(s->comment());
            cf_def.__set_bloom_filter_fp_chance(s->bloom_filter_fp_chance());
            if (s->regular_columns_count()) {
                std::vector<ColumnDef> columns;
                for (auto&& c : s->regular_columns()) {
                    ColumnDef c_def;
                    c_def.__set_name(c.name_as_text());
                    c_def.__set_validation_class(class_from_data_type(c.type));
                    columns.emplace_back(std::move(c_def));
                }
                cf_def.__set_column_metadata(columns);
            }
            // FIXME: there are more fields that should be filled...
            cfs.emplace_back(std::move(cf_def));
        }
        def.__set_cf_defs(cfs);
        def.__set_durable_writes(meta->durable_writes());
        return std::move(def);
    }
    static column_family& lookup_column_family(database& db, const sstring& ks_name, const sstring& cf_name) {
        if (ks_name.empty()) {
            throw make_exception<InvalidRequestException>("keyspace not set");
        }
        try {
            return db.find_column_family(ks_name, cf_name);
        } catch (no_such_column_family&) {
            throw make_exception<InvalidRequestException>("column family %s not found", cf_name);
        }
    }
    static schema_ptr lookup_schema(database& db, const sstring& ks_name, const sstring& cf_name) {
        return lookup_column_family(db, ks_name, cf_name).schema();
    }
    static partition_key key_from_thrift(const schema& s, const bytes& k) {
        thrift_validation::validate_key(s, k);
        if (s.partition_key_size() == 1) {
            return partition_key::from_single_value(s, k);
        }
        return partition_key::from_exploded(legacy_compound_type::select_values(
                    legacy_compound_type::parse(*s.partition_key_type(), k)));
    }
    static db::consistency_level cl_from_thrift(const ConsistencyLevel::type consistency_level) {
        switch(consistency_level) {
        case ConsistencyLevel::type::ONE: return db::consistency_level::ONE;
        case ConsistencyLevel::type::QUORUM: return db::consistency_level::QUORUM;
        case ConsistencyLevel::type::LOCAL_QUORUM: return db::consistency_level::LOCAL_QUORUM;
        case ConsistencyLevel::type::EACH_QUORUM: return db::consistency_level::EACH_QUORUM;
        case ConsistencyLevel::type::ALL: return db::consistency_level::ALL;
        case ConsistencyLevel::type::ANY: return db::consistency_level::ANY;
        case ConsistencyLevel::type::TWO: return db::consistency_level::TWO;
        case ConsistencyLevel::type::THREE: return db::consistency_level::THREE;
        case ConsistencyLevel::type::SERIAL: return db::consistency_level::SERIAL;
        case ConsistencyLevel::type::LOCAL_SERIAL: return db::consistency_level::LOCAL_SERIAL;
        case ConsistencyLevel::type::LOCAL_ONE: return db::consistency_level::LOCAL_ONE;
        default: throw make_exception<InvalidRequestException>("undefined consistency_level %s", consistency_level);
        }
    }
    static ttl_opt maybe_ttl(const schema& s, const Column& col) {
        if (col.__isset.ttl) {
            auto ttl = std::chrono::duration_cast<gc_clock::duration>(std::chrono::seconds(col.ttl));
            if (ttl.count() <= 0) {
                throw make_exception<InvalidRequestException>("ttl must be positive");
            }
            if (ttl > max_ttl) {
                throw make_exception<InvalidRequestException>("ttl is too large");
            }
            return {ttl};
        }
        return { };
    }
    static clustering_key_prefix make_clustering_prefix(const schema& s, bytes v) {
        if (s.clustering_key_size() == 1) {
            return clustering_key_prefix::from_single_value(s, v);
        }
        return clustering_key_prefix::from_exploded(legacy_compound_type::select_values(
                    legacy_compound_type::parse(*s.clustering_key_type(), v)));
    }
    static bool is_dynamic(const schema& s) {
        // FIXME: what about CFs created from CQL?
        return s.clustering_key_size() > 0;
    }
    static query::clustering_range make_clustering_range(const schema& s, const std::string& start, const std::string& end) {
        using bound = query::clustering_range::bound;
        stdx::optional<bound> start_bound;
        if (!start.empty()) {
            start_bound = bound(make_clustering_prefix(s, to_bytes(start)));
        }
        stdx::optional<bound> end_bound;
        if (!end.empty()) {
            end_bound = bound(make_clustering_prefix(s, to_bytes(end)));
        }
        query::clustering_range range = {std::move(start_bound), std::move(end_bound)};
        if (range.is_wrap_around(clustering_key_prefix::prefix_equal_tri_compare(s))) {
            throw make_exception<InvalidRequestException>("Range finish must come after start in the order of traversal");
        }
        return range;
    }
    static std::pair<schema::const_iterator, schema::const_iterator> make_column_range(const schema& s, const std::string& start, const std::string& end) {
        auto start_it = start.empty() ? s.regular_begin() : s.regular_lower_bound(to_bytes(start));
        auto end_it = end.empty() ? s.regular_end() : s.regular_upper_bound(to_bytes(end));
        if (start_it > end_it) {
            throw make_exception<InvalidRequestException>("Range finish must come after start in the order of traversal");
        }
        return std::make_pair(std::move(start_it), std::move(end_it));
    }
    static void add_columns(auto&& beg, auto&& end, std::vector<column_id>& out, uint32_t count, bool reversed) {
         while (beg != end && count-- > 0) {
            auto&& c = reversed ? *--end : *beg++;
            if (c.is_atomic()) {
                out.emplace_back(c.id);
            }
        }
    }
    static query::partition_slice::option_set query_opts(const schema& s) {
        query::partition_slice::option_set opts;
        opts.set(query::partition_slice::option::send_timestamp);
        opts.set(query::partition_slice::option::send_ttl);
        if (is_dynamic(s)) {
            opts.set(query::partition_slice::option::send_clustering_key);
        }
        opts.set(query::partition_slice::option::send_partition_key);
        return opts;
    }
    static lw_shared_ptr<query::read_command> slice_pred_to_read_cmd(const schema& s, const SlicePredicate& predicate) {
        auto opts = query_opts(s);
        std::vector<query::clustering_range> clustering_ranges;
        std::vector<column_id> regular_columns;
        uint32_t per_partition_row_limit = query::max_rows;
        if (predicate.__isset.column_names) {
            thrift_validation::validate_column_names(predicate.column_names);
            std::vector<std::string> unique_column_names;
            boost::copy(predicate.column_names | boost::adaptors::uniqued, std::back_inserter(unique_column_names));
            if (is_dynamic(s)) {
                for (auto&& name : unique_column_names) {
                    auto ckey = make_clustering_prefix(s, to_bytes(name));
                    clustering_ranges.emplace_back(query::clustering_range::make_singular(std::move(ckey)));
                }
                regular_columns.emplace_back(s.regular_begin()->id);
            } else {
                clustering_ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
                auto&& defs = unique_column_names
                    | boost::adaptors::transformed([&s](auto&& name) { return s.get_column_definition(to_bytes(name)); })
                    | boost::adaptors::filtered([](auto* def) { return def; });
                add_columns(boost::make_indirect_iterator(defs.begin()), boost::make_indirect_iterator(defs.end()),
                        regular_columns, query::max_rows, false);
            }
        } else if (predicate.__isset.slice_range) {
            auto range = predicate.slice_range;
            if (range.count < 0) {
                throw make_exception<InvalidRequestException>("SliceRange requires non-negative count");
            }
            if (range.reversed) {
                std::swap(range.start, range.finish);
                opts.set(query::partition_slice::option::reversed);
            }
            per_partition_row_limit = static_cast<uint32_t>(range.count);
            if (is_dynamic(s)) {
                clustering_ranges.emplace_back(make_clustering_range(s, range.start, range.finish));
                regular_columns.emplace_back(s.regular_begin()->id);
            } else {
                clustering_ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
                auto r = make_column_range(s, range.start, range.finish);
                add_columns(r.first, r.second, regular_columns, per_partition_row_limit, range.reversed);
            }
        } else {
            throw make_exception<InvalidRequestException>("SlicePredicate column_names and slice_range may not both be null");
        }
        auto slice = query::partition_slice(std::move(clustering_ranges), {}, std::move(regular_columns), opts,
                nullptr, cql_serialization_format::internal(), per_partition_row_limit);
        return make_lw_shared<query::read_command>(s.id(), s.version(), std::move(slice));
    }
    static ColumnParent column_path_to_column_parent(const ColumnPath& column_path) {
        ColumnParent ret;
        ret.__set_column_family(column_path.column_family);
        if (column_path.__isset.super_column) {
            ret.__set_super_column(column_path.super_column);
        }
        return ret;
    }
    static SlicePredicate column_path_to_slice_predicate(const ColumnPath& column_path) {
        SlicePredicate ret;
        if (column_path.__isset.column) {
            ret.__set_column_names({column_path.column});
        }
        return ret;
    }
    static std::vector<query::partition_range> make_partition_ranges(const schema& s, const std::vector<std::string>& keys) {
        std::vector<query::partition_range> ranges;
        for (auto&& key : keys) {
            auto pk = key_from_thrift(s, to_bytes(key));
            auto dk = dht::global_partitioner().decorate_key(s, pk);
            ranges.emplace_back(query::partition_range::make_singular(std::move(dk)));
        }
        return ranges;
    }
    static Column make_column(const bytes& col, const query::result_atomic_cell_view& cell) {
        Column ret;
        ret.__set_name(bytes_to_string(col));
        ret.__set_value(bytes_to_string(cell.value()));
        ret.__set_timestamp(cell.timestamp());
        if (cell.ttl()) {
            ret.__set_ttl(cell.ttl()->count());
        }
        return ret;
    }
    static ColumnOrSuperColumn column_to_column_or_supercolumn(Column&& col) {
        ColumnOrSuperColumn ret;
        ret.__set_column(std::move(col));
        return ret;
    }
    static ColumnOrSuperColumn make_column_or_supercolumn(const bytes& col, const query::result_atomic_cell_view& cell) {
        return column_to_column_or_supercolumn(make_column(col, cell));
    }
    static std::string partition_key_to_string(const schema& s, const partition_key& key) {
        return bytes_to_string(*key.begin(s));
    }
    template<typename Aggregator>
    class column_visitor : public Aggregator {
        const schema& _s;
        const query::partition_slice& _slice;
        std::map<std::string, typename Aggregator::type> _aggregator;
        typename Aggregator::type* _current_aggregator;
    public:
        column_visitor(const schema& s, const query::partition_slice& slice)
                : _s(s), _slice(slice)
        { }
        std::map<std::string, typename Aggregator::type>&& release() {
            return std::move(_aggregator);
        }
        void accept_new_partition(const partition_key& key, uint32_t row_count) {
            _current_aggregator = &_aggregator[partition_key_to_string(_s, key)];
        }
        void accept_new_partition(uint32_t row_count) {
            abort();
        }
        void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
            auto cell = row.iterator().next_atomic_cell();
            if (cell) {
                Aggregator::on_column(_current_aggregator, key.explode()[0], *cell);
            }
        }
        void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
            auto&& it = row.iterator();
            for (auto&& id : _slice.regular_columns) {
                auto cell = it.next_atomic_cell();
                if (cell) {
                    Aggregator::on_column(_current_aggregator, _s.regular_column_at(id).name(), *cell);
                }
            }
        }
        void accept_partition_end(const query::result_row_view& static_row) {
        }
    };
    struct column_or_supercolumn_builder {
        using type = std::vector<ColumnOrSuperColumn>;
        void on_column(std::vector<ColumnOrSuperColumn>* current_cols, const bytes& name, const query::result_atomic_cell_view& cell) {
            current_cols->emplace_back(make_column_or_supercolumn(name, cell));
        }
    };
    using column_aggregator = column_visitor<column_or_supercolumn_builder>;
    struct counter {
        using type = int32_t;
        void on_column(int32_t* current_cols, const bytes& name, const query::result_atomic_cell_view& cell) {
            *current_cols += 1;
        }
    };
    using column_counter = column_visitor<counter>;
    static query::partition_range make_partition_range(const schema& s, const KeyRange& range) {
        if (range.__isset.row_filter) {
            // FIXME: implement secondary indexes
            throw unimplemented_exception();
        }
        if ((range.__isset.start_key == range.__isset.start_token)
                || (range.__isset.end_key == range.__isset.end_token)) {
            throw make_exception<InvalidRequestException>(
                    "Exactly one each of {start key, start token} and {end key, end token} must be specified");
        }
        if (range.__isset.start_token && range.__isset.end_key) {
            throw make_exception<InvalidRequestException>("Start token + end key is not a supported key range");
        }

        auto &&partitioner = dht::global_partitioner();

        if (range.__isset.start_key && range.__isset.end_key) {
            auto start = range.start_key.empty()
                       ? dht::ring_position::starting_at(dht::minimum_token())
                       : partitioner.decorate_key(s, key_from_thrift(s, to_bytes(range.start_key)));
            auto end = range.end_key.empty()
                     ? dht::ring_position::ending_at(dht::maximum_token())
                     : partitioner.decorate_key(s, key_from_thrift(s, to_bytes(range.end_key)));
            if (end.less_compare(s, start) && !end.token().is_maximum()) {
                if (partitioner.preserves_order()) {
                    throw make_exception<InvalidRequestException>(
                            "Start key must sort before (or equal to) finish key in the partitioner");
                } else {
                    throw make_exception<InvalidRequestException>(
                            "Start key's token sorts after end key's token. This is not allowed; you probably should not specify end key at all except with an ordered partitioner");
                }
            }
            return {query::partition_range::bound(std::move(start), true),
                    query::partition_range::bound(std::move(end), true)};
        }

        if (range.__isset.start_key && range.__isset.end_token) {
            // start_token/end_token can wrap, but key/token should not
            auto start = range.start_key.empty()
                       ? dht::ring_position::starting_at(dht::minimum_token())
                       : partitioner.decorate_key(s, key_from_thrift(s, to_bytes(range.start_key)));
            auto end = dht::ring_position::ending_at(partitioner.from_sstring(sstring(range.end_token)));
            if (end.token().is_minimum()) {
                end = dht::ring_position::ending_at(dht::maximum_token());
            } else if (end.less_compare(s, start)) {
                throw make_exception<InvalidRequestException>("Start key's token sorts after end token");
            }
            return {query::partition_range::bound(std::move(start), true),
                    query::partition_range::bound(std::move(end), false)};
        }

        // Token range can wrap.
        auto start = dht::ring_position::starting_at(partitioner.from_sstring(sstring(range.start_token)));
        auto end = dht::ring_position::ending_at(partitioner.from_sstring(sstring(range.end_token)));
        if (end.token().is_minimum()) {
            end = dht::ring_position::ending_at(dht::maximum_token());
        }
        if (start.token() == end.token()) {
            return query::partition_range::make_open_ended_both_sides();
        }
        return {query::partition_range::bound(std::move(start), false),
                query::partition_range::bound(std::move(end), false)};
    }
    static std::vector<KeySlice> to_key_slices(const schema& s, const query::partition_slice& slice, query::result_view v) {
        column_aggregator aggregator(s, slice);
        v.consume(slice, aggregator);
        auto&& cols = aggregator.release();
        std::vector<KeySlice> ret;
        std::transform(
                std::make_move_iterator(cols.begin()),
                std::make_move_iterator(cols.end()),
                boost::back_move_inserter(ret),
                [](auto&& p) {
            KeySlice ks;
            ks.__set_key(std::move(p.first));
            ks.__set_columns(std::move(p.second));
            return ks;
        });
        return ret;
    }
};

class handler_factory : public CassandraCobSvIfFactory {
    distributed<database>& _db;
public:
    explicit handler_factory(distributed<database>& db) : _db(db) {}
    typedef CassandraCobSvIf Handler;
    virtual CassandraCobSvIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) {
        return new thrift_handler(_db);
    }
    virtual void releaseHandler(CassandraCobSvIf* handler) {
        delete handler;
    }
};

std::unique_ptr<CassandraCobSvIfFactory>
create_handler_factory(distributed<database>& db) {
    return std::make_unique<handler_factory>(db);
}
