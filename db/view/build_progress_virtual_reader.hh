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

#include "database.hh"
#include "db/system_keyspace.hh"
#include "db/timeout_clock.hh"
#include "dht/i_partitioner.hh"
#include "flat_mutation_reader.hh"
#include "mutation_fragment.hh"
#include "mutation_reader.hh"
#include "query-request.hh"
#include "schema.hh"
#include "tracing/tracing.hh"

#include <boost/range/iterator_range.hpp>

#include <iterator>
#include <memory>

namespace db::view {

class build_progress_virtual_reader {
    database& _db;

    struct build_progress_reader : flat_mutation_reader::impl {
        flat_mutation_reader _underlying;

        build_progress_reader(schema_ptr schema,flat_mutation_reader underlying)
                : flat_mutation_reader::impl(std::move(schema))
                , _underlying(std::move(underlying)) {
        }

        clustering_key adjust_ckey(clustering_key& ck) {
            if (ck.size(*_schema) < 3) {
                return std::move(ck);
            }
            // Drop the cpu_id from the clustering key
            auto end = ck.begin(*_schema);
            std::advance(end, 1);
            auto r = boost::make_iterator_range(ck.begin(*_schema), std::move(end));
            return clustering_key_prefix::from_exploded(r);
        }

        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            return _underlying.fill_buffer(timeout).then([this] {
                _end_of_stream = _underlying.is_end_of_stream();
                while (!_underlying.is_buffer_empty()) {
                    auto mf = _underlying.pop_mutation_fragment();
                    if (mf.is_clustering_row()) {
                        auto scylla_in_progress_row = std::move(mf).as_clustering_row();
                        auto compat_in_progress_row = row();
                        // Drop the first_token from the regular columns
                        compat_in_progress_row.append_cell(0, scylla_in_progress_row.cells().cell_at(0)); // next_token
                        compat_in_progress_row.append_cell(1, scylla_in_progress_row.cells().cell_at(1)); // generation_number
                        mf = clustering_row(
                                adjust_ckey(scylla_in_progress_row.key()),
                                std::move(scylla_in_progress_row.tomb()),
                                std::move(scylla_in_progress_row.marker()),
                                std::move(compat_in_progress_row));
                    } else if (mf.is_range_tombstone()) {
                        auto scylla_in_progress_rt = std::move(mf).as_range_tombstone();
                        mf = range_tombstone(
                                adjust_ckey(scylla_in_progress_rt.start),
                                scylla_in_progress_rt.start_kind,
                                scylla_in_progress_rt.end,
                                scylla_in_progress_rt.end_kind,
                                scylla_in_progress_rt.tomb);
                    }
                    push_mutation_fragment(mf);
                }
            });
        }

        virtual void next_partition() override {
            _end_of_stream = false;
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
                _underlying.next_partition();
            }
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            return _underlying.fast_forward_to(pr, timeout);
        }

        virtual future<> fast_forward_to(position_range range, db::timeout_clock::time_point timeout) override {
            return _underlying.fast_forward_to(std::move(range), timeout);
        }
    };

public:
    build_progress_virtual_reader(database& db)
            : _db(db) {
    }

    flat_mutation_reader operator()(schema_ptr s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        auto& db = service::get_local_storage_proxy().get_db().local();
        auto& scylla_views_build_progress = db.find_column_family(s->ks_name(), system_keyspace::v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS);
        return flat_mutation_reader(std::make_unique<build_progress_reader>(
                scylla_views_build_progress.schema(),
                scylla_views_build_progress.make_reader(
                        scylla_views_build_progress.schema(),
                        range,
                        slice,
                        pc,
                        std::move(trace_state),
                        fwd,
                        fwd_mr)));
    }
};

}