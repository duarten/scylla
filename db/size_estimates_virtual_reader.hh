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

#include <boost/range/adaptor/indirected.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/find_if.hpp>

#include "database.hh"
#include "db/system_keyspace.hh"
#include "dht/i_partitioner.hh"
#include "mutation_reader.hh"
#include "partition_range_compat.hh"
#include "range.hh"
#include "service/storage_service.hh"
#include "stdx.hh"
#include "streamed_mutation.hh"

namespace db {

namespace size_estimates {

class size_estimates_mutation_reader final : public mutation_reader::impl {
    struct utf8_tri_comparator {
        int operator()(const sstring& s1, const sstring& s2) const {
            return utf8_type->compare(to_bytes_view(s1), to_bytes_view(s2));
        }
    };
    struct utf8_less_comparator {
        bool operator()(const sstring& s1, const sstring& s2) const {
            return utf8_tri_comparator()(s1, s2) < 0;
        }
    };
    struct token_range {
        sstring start;
        sstring end;

        struct start_comparator {
            int operator()(const token_range& r, const sstring& token) const {
                return utf8_tri_comparator()(r.start, token);
            }
            int operator()(const sstring& token, const token_range& r) const {
                return -operator()(r, token);
            }
            bool operator()(const token_range& r1, const token_range& r2) const {
                return utf8_less_comparator()(r1.start, r2.start);
            }
        };
        struct end_comparator {
            int operator()(const token_range& r, const sstring& token) const {
                return utf8_tri_comparator()(r.end, token);
            }
            int operator()(const sstring& token, const token_range& r) const {
                return -operator()(r, token);
            }
        };
    };
    schema_ptr _schema;
    const query::partition_range& _prange;
    const query::partition_slice& _slice;
    using ks_range = std::vector<lw_shared_ptr<keyspace_metadata>>;
    stdx::optional<ks_range> _keyspaces;
    ks_range::const_iterator _current_partition;
public:
    size_estimates_mutation_reader(schema_ptr schema, const query::partition_range& prange, const query::partition_slice& slice)
            : _schema(schema)
            , _prange(prange)
            , _slice(slice)
    { }

    virtual future<streamed_mutation_opt> operator()() override {
        // For each specified range, estimate (crudely) mean partition size and partitions count.
        return seastar::async([this] {
            auto& db = service::get_local_storage_proxy().get_db().local();
            if (!_keyspaces) {
                _keyspaces = get_keyspaces(*_schema, db, _prange);
                _current_partition = _keyspaces->begin();
            }
            streamed_mutation_opt smo;
            if (_current_partition != _keyspaces->end()) {
                smo = streamed_mutation_opt(streamed_mutation_from_mutation(estimate_for_current_keyspace(db)));
                ++_current_partition;
            }
            return smo;
        });
    }
    /**
     * Returns the primary ranges for the local node.
     * Requires running in the context of a seastar thread, due to storage_service::get_local_tokens().
     * Used for testing as well.
     */
    static std::vector<token_range> get_local_ranges() {
        auto& ss = service::get_local_storage_service();
        auto ranges = ss.get_token_metadata().get_primary_ranges_for(ss.get_local_tokens());
        boost::sort(ranges, nonwrapping_range<dht::token>::less_comparator_by_start(dht::token_comparator()));
        // We merge the ranges to be compatible with how Cassandra shows it's size estimates table.
        // All queries will be on that table, where all entries are text and there's no notion of
        // token ranges form the CQL point of view.
        auto wrapping_ranges = compat::wrap(std::move(ranges));
        auto local_ranges = boost::copy_range<std::vector<token_range>>(wrapping_ranges | boost::adaptors::transformed([] (auto&& r) {
            auto to_str = [](auto&& b) { return dht::global_partitioner().to_sstring(b->value()); };
            return token_range{ to_str(r.start()), to_str(r.end()) };
        }));
        boost::sort(local_ranges, token_range::start_comparator());
        return local_ranges;
    }
private:
    mutation estimate_for_current_keyspace(const database& db) const {
        auto keyspace = *_current_partition;
        auto local_ranges = get_local_ranges(); // Needs to be called in the context of a seastar thread. Sorted by start bound.
        auto cf_names = boost::copy_range<std::vector<sstring>>(keyspace->cf_meta_data() | boost::adaptors::map_keys);
        boost::sort(cf_names, utf8_less_comparator());
        std::vector<db::system_keyspace::range_estimates> estimates;
        for (auto& range : _slice.get_all_ranges()) {
            nonwrapping_range<sstring> cf_range, start_token_range, end_token_range;
            std::tie(cf_range, start_token_range, end_token_range) = extract_components(range);
            auto restricted_cf_names = clamp(cf_names, cf_range, utf8_tri_comparator());
            auto ranges = clamp(local_ranges, start_token_range, token_range::start_comparator());
            if (!end_token_range.is_full()) {
                ranges = clamp(std::move(ranges), end_token_range, token_range::end_comparator());
            }
            for (auto cf_name : restricted_cf_names) {
                auto schema = keyspace->cf_meta_data().at(cf_name);
                auto& cf = db.find_column_family(schema);
                for (auto&& r : ranges) {
                    estimate(schema, cf, r, estimates);
                    if (estimates.size() >= _slice.partition_row_limit()) {
                        break;
                    }
                }
            }
        }
        return db::system_keyspace::make_size_estimates_mutation(keyspace->name(), std::move(estimates));
    }
    /**
     * The partition slice encodes a range of keyspace names. We support only a singular range,
     * and return the corresponding keyspace object.
     */
    static ks_range get_keyspaces(const schema& s, const database& db, query::partition_range range) {
        struct keyspace_comparator {
            const schema& _s;
            dht::ring_position_comparator _cmp;
            keyspace_comparator(const schema& s)
                    : _s(s)
                    , _cmp(_s)
            { }
            int operator()(lw_shared_ptr<keyspace_metadata> ks, const dht::ring_position& rp) {
                auto krp = as_ring_position(*ks);
                return _cmp(krp, rp);
            }
            int operator()(const dht::ring_position& rp, lw_shared_ptr<keyspace_metadata> ks) {
                return -operator()(ks, rp);
            }
            bool operator()(lw_shared_ptr<keyspace_metadata> ks1, lw_shared_ptr<keyspace_metadata> ks2) {
                auto krp1 = as_ring_position(*ks1);
                auto krp2 = as_ring_position(*ks2);
                return _cmp(krp1, krp2) < 0;
            }
            dht::ring_position as_ring_position(const keyspace_metadata& ks) {
                return dht::global_partitioner().decorate_key(_s, partition_key::from_single_value(_s, to_bytes(ks.name())));
            }
        };
        keyspace_comparator cmp(s);
        auto keyspaces = boost::copy_range<ks_range>(db.get_keyspaces() | boost::adaptors::transformed([](auto&& ks) {
            return ks.second.metadata();
        }));
        boost::sort(keyspaces, cmp);
        return clamp(std::move(keyspaces), range, std::move(cmp));
    }

    /**
     * Returns a subset of the range that is within the specified limits.
     */
    template<typename T, typename U, typename Comparator>
    static std::vector<T> clamp(std::vector<T> range, nonwrapping_range<U>& limits, Comparator&& tri_cmp) {
        std::vector<T> res;

        if (limits.is_singular()) {
            auto it = boost::find_if(range, [&](auto&& v) { return tri_cmp(limits.start()->value(), v) == 0; });
            if (it != range.end()) {
                res.push_back(std::move(*it));
            }
            return res;
        }

        auto less_cmp = [&tri_cmp](auto&& x, auto&& y) {
            return tri_cmp(x, y) < 0;
        };

        typename std::vector<T>::iterator start;
        if (limits.start()) {
            start = std::lower_bound(range.begin(), range.end(), limits.start()->value(), less_cmp);
            if (start == range.end()) {
                start = range.begin();
            } else if (!limits.start()->is_inclusive() && tri_cmp(limits.start()->value(), *start) == 0) {
                ++start;
            }
        } else {
            start = range.begin();
        }

        typename std::vector<T>::iterator end;
        if (limits.end()) {
            end = std::upper_bound(range.begin(), range.end(), limits.end()->value(), less_cmp);
            if (end != range.begin() && !limits.end()->is_inclusive() && range.begin() != end && tri_cmp(limits.end()->value(), *std::prev(end)) == 0) {
                --end;
            }
        } else {
            end = range.end();
        }

        if (end > start) {
            std::move(start, end, std::back_inserter(res));
        }

        return res;
    }

    /**
     * Extracts the query arguments from the specified clustering range. These include the table name, the
     * start token and the end token. We expect a range of these components.
     */
    static std::tuple<nonwrapping_range<sstring>, nonwrapping_range<sstring>, nonwrapping_range<sstring>>
    extract_components(const query::clustering_range range) {
        auto process_bound = [](const query::clustering_range::bound& bound) {
            auto components = bound.value().components();
            auto components_it = components.begin();
            auto end = components.end();
            assert(components_it != end);
            bool inclusive_cf_name, inclusive_start_bound, inclusive_end_bound;
            inclusive_cf_name = inclusive_start_bound = inclusive_end_bound = bound.is_inclusive();
            auto cf_name = value_cast<sstring>(utf8_type->deserialize(*components_it++));
            stdx::optional<nonwrapping_range<sstring>::bound> start_bound, end_bound;
            if (components_it != end) {
                inclusive_cf_name = true;
                auto start_token = value_cast<sstring>(utf8_type->deserialize(*components_it++));
                if (components_it != end) {
                    inclusive_start_bound = true;
                    end_bound = {{ value_cast<sstring>(utf8_type->deserialize(*components_it)), inclusive_end_bound }};
                }
                start_bound = {{ std::move(start_token), inclusive_start_bound }};
            }
            return std::make_tuple(nonwrapping_range<sstring>::bound(cf_name, inclusive_cf_name), std::move(start_bound), std::move(end_bound));
        };

        stdx::optional<nonwrapping_range<sstring>::bound> start_cf_name, end_cf_name;
        stdx::optional<nonwrapping_range<sstring>::bound> left_start_token_range, left_end_token_range;
        stdx::optional<nonwrapping_range<sstring>::bound> right_start_token_range, right_end_token_range;

        if (range.start()) {
            std::tie(start_cf_name, left_start_token_range, right_start_token_range) = process_bound(*range.start());
        }
        if (range.end()) {
            std::tie(end_cf_name, left_end_token_range, right_end_token_range) = process_bound(*range.end());
        }

        bool cf_name_range_singular = range.is_singular() && start_cf_name && !end_cf_name;
        bool left_token_range_singular = range.is_singular() && left_start_token_range && !left_end_token_range;
        bool right_token_range_singular = range.is_singular() && right_start_token_range && !right_end_token_range;
        return std::make_tuple(
            nonwrapping_range<sstring>(std::move(start_cf_name), std::move(end_cf_name), cf_name_range_singular),
            nonwrapping_range<sstring>(std::move(left_start_token_range), std::move(left_end_token_range), left_token_range_singular),
            nonwrapping_range<sstring>(std::move(right_start_token_range), std::move(right_end_token_range), right_token_range_singular));
    }

    /**
     * Makes a wrapping range of ring_position from a nonwrapping range of token, used to select sstables.
     */
    static nonwrapping_range<dht::ring_position> as_ring_position_range(nonwrapping_range<dht::token>& r) {
        stdx::optional<range<dht::ring_position>::bound> start_bound, end_bound;
        if (r.start()) {
            start_bound = {{ dht::ring_position(r.start()->value(), dht::ring_position::token_bound::start), r.start()->is_inclusive() }};
        }
        if (r.end()) {
            end_bound = {{ dht::ring_position(r.end()->value(), dht::ring_position::token_bound::end), r.end()->is_inclusive() }};
        }
        return nonwrapping_range<dht::ring_position>(std::move(start_bound), std::move(end_bound), r.is_singular());
    }

    /**
     * Add a new range_estimates for the specified range, considering the sstables associated with `cf`.
     */
    static void estimate(schema_ptr schema, const column_family& cf, const token_range& r, std::vector<system_keyspace::range_estimates>& estimates) {
        int64_t count{0};
        utils::estimated_histogram hist{0};
        auto from_str = [](auto&& str) { return dht::global_partitioner().from_sstring(str); };
        std::vector<nonwrapping_range<dht::token>> ranges;
        compat::unwrap_into(
            wrapping_range<dht::token>({{ from_str(r.start) }}, {{ from_str(r.end) }}),
            dht::token_comparator(),
            [&] (auto&& rng) { ranges.push_back(std::move(rng)); });
        for (auto&& r : ranges) {
            auto rp_range = as_ring_position_range(r);
            for (auto&& sstable : cf.select_sstables(rp_range)) {
                count += sstable->estimated_keys_for_range(r);
                hist.merge(sstable->get_stats_metadata().estimated_row_size);
            }
        }
        estimates.push_back({schema, r.start, r.end, count, count > 0 ? hist.mean() : 0});
    }
};

struct virtual_reader {
    mutation_reader operator()(schema_ptr schema,
            const query::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state) {
        return make_mutation_reader<size_estimates_mutation_reader>(schema, range, slice);
    }
};

} // namespace size_estimates

} // namespace db
