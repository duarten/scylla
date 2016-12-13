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

#include <boost/range/adaptor/reversed.hpp>
#include <seastar/util/defer.hh>
#include "mutation_partition.hh"
#include "mutation_partition_applier.hh"
#include "converting_mutation_partition_applier.hh"
#include "partition_builder.hh"
#include "query-result-writer.hh"
#include "atomic_cell_hash.hh"
#include "reversibly_mergeable.hh"
#include "streamed_mutation.hh"
#include "mutation_query.hh"
#include "service/priority_manager.hh"
#include "mutation_compactor.hh"

template<bool reversed>
struct reversal_traits;

template<>
struct reversal_traits<false> {
    template <typename Container>
    static auto begin(Container& c) {
        return c.begin();
    }

    template <typename Container>
    static auto end(Container& c) {
        return c.end();
    }

    template <typename Container, typename Disposer>
    static typename Container::iterator erase_and_dispose(Container& c,
        typename Container::iterator begin,
        typename Container::iterator end,
        Disposer disposer)
    {
        return c.erase_and_dispose(begin, end, std::move(disposer));
    }

    template<typename Container, typename Disposer>
    static typename Container::iterator erase_dispose_and_update_end(Container& c,
         typename Container::iterator it, Disposer&& disposer,
         typename Container::iterator&)
    {
        return c.erase_and_dispose(it, std::forward<Disposer>(disposer));
    }

    template <typename Container>
    static boost::iterator_range<typename Container::iterator> maybe_reverse(
        Container& c, boost::iterator_range<typename Container::iterator> r)
    {
        return r;
    }

    template <typename Container>
    static typename Container::iterator maybe_reverse(Container&, typename Container::iterator r) {
        return r;
    }
};

template<>
struct reversal_traits<true> {
    template <typename Container>
    static auto begin(Container& c) {
        return c.rbegin();
    }

    template <typename Container>
    static auto end(Container& c) {
        return c.rend();
    }

    template <typename Container, typename Disposer>
    static typename Container::reverse_iterator erase_and_dispose(Container& c,
        typename Container::reverse_iterator begin,
        typename Container::reverse_iterator end,
        Disposer disposer)
    {
        return typename Container::reverse_iterator(
            c.erase_and_dispose(end.base(), begin.base(), disposer)
        );
    }

    // Erases element pointed to by it and makes sure than iterator end is not
    // invalidated.
    template<typename Container, typename Disposer>
    static typename Container::reverse_iterator erase_dispose_and_update_end(Container& c,
        typename Container::reverse_iterator it, Disposer&& disposer,
        typename Container::reverse_iterator& end)
    {
        auto to_erase = std::next(it).base();
        bool update_end = end.base() == to_erase;
        auto ret = typename Container::reverse_iterator(
            c.erase_and_dispose(to_erase, std::forward<Disposer>(disposer))
        );
        if (update_end) {
            end = ret;
        }
        return ret;
    }

    template <typename Container>
    static boost::iterator_range<typename Container::reverse_iterator> maybe_reverse(
        Container& c, boost::iterator_range<typename Container::iterator> r)
    {
        using reverse_iterator = typename Container::reverse_iterator;
        return boost::make_iterator_range(reverse_iterator(r.end()), reverse_iterator(r.begin()));
    }

    template <typename Container>
    static typename Container::reverse_iterator maybe_reverse(Container&, typename Container::iterator r) {
        return typename Container::reverse_iterator(r);
    }
};


//
// apply_reversibly_intrusive_set() and revert_intrusive_set() implement ReversiblyMergeable
// for a boost::intrusive_set<> container of ReversiblyMergeable entries.
//
// See reversibly_mergeable.hh
//
// Requirements:
//  - entry has distinct key and value states
//  - entries are ordered only by key in the container
//  - entry can have an empty value
//  - presence of an entry with an empty value doesn't affect equality of the containers
//  - E::empty() returns true iff the value is empty
//  - E(e.key()) creates an entry with empty value but the same key as that of e.
//
// Implementation of ReversiblyMergeable for the entry's value is provided via Apply and Revert functors.
//
// ReversiblyMergeable is constructed assuming the following properties of the 'apply' operation
// on containers:
//
//  apply([{k1, v1}], [{k1, v2}]) = [{k1, apply(v1, v2)}]
//  apply([{k1, v1}], [{k2, v2}]) = [{k1, v1}, {k2, v2}]
//

// revert for apply_reversibly_intrusive_set()
template<typename Container, typename Revert = default_reverter<typename Container::value_type>>
void revert_intrusive_set_range(Container& dst, Container& src,
    typename Container::iterator start,
    typename Container::iterator end,
    Revert&& revert = Revert()) noexcept
{
    using value_type = typename Container::value_type;
    auto deleter = current_deleter<value_type>();
    while (start != end) {
        auto& e = *start;
        // lower_bound() can allocate if linearization is required but it should have
        // been already performed by the lower_bound() invocation in apply_reversibly_intrusive_set() and
        // stored in the linearization context.
        auto i = dst.find(e);
        assert(i != dst.end());
        value_type& dst_e = *i;

        if (e.empty()) {
            dst.erase(i);
            start = src.erase_and_dispose(start, deleter);
            start = src.insert_before(start, dst_e);
        } else {
            revert(dst_e, e);
        }

        ++start;
    }
}

template<typename Container, typename Revert = default_reverter<typename Container::value_type>>
void revert_intrusive_set(Container& dst, Container& src, Revert&& revert = Revert()) noexcept {
    revert_intrusive_set_range(dst, src, src.begin(), src.end(), std::forward<Revert>(revert));
}

// Applies src onto dst. See comment above revert_intrusive_set_range() for more details.
//
// Returns an object which upon going out of scope, unless cancel() is called on it,
// reverts the applicaiton by calling revert_intrusive_set(). The references to containers
// must be stable as long as the returned object is live.
template<typename Container,
        typename Apply = default_reversible_applier<typename Container::value_type>,
        typename Revert = default_reverter<typename Container::value_type>>
auto apply_reversibly_intrusive_set(Container& dst, Container& src, Apply&& apply = Apply(), Revert&& revert = Revert()) {
    using value_type = typename Container::value_type;
    auto src_i = src.begin();
    try {
        while (src_i != src.end()) {
            value_type& src_e = *src_i;

            // neutral entries will be given special meaning for the purpose of revert, so
            // get rid of empty rows from the input as if they were not there. This doesn't change
            // the value of src.
            if (src_e.empty()) {
                src_i = src.erase_and_dispose(src_i, current_deleter<value_type>());
                continue;
            }

            auto i = dst.lower_bound(src_e);
            if (i == dst.end() || dst.key_comp()(src_e, *i)) {
                // Construct neutral entry which will represent missing dst entry for revert.
                value_type* empty_e = current_allocator().construct<value_type>(src_e.key());
                [&] () noexcept {
                    src_i = src.erase(src_i);
                    src_i = src.insert_before(src_i, *empty_e);
                    dst.insert_before(i, src_e);
                }();
            } else {
                apply(*i, src_e);
            }
            ++src_i;
        }
        return defer([&dst, &src, revert] { revert_intrusive_set(dst, src, revert); });
    } catch (...) {
        revert_intrusive_set_range(dst, src, src.begin(), src_i, revert);
        throw;
    }
}

mutation_partition::mutation_partition(const mutation_partition& x)
        : _tombstone(x._tombstone)
        , _static_row(x._static_row)
        , _rows(x._rows.value_comp())
        , _row_tombstones(x._row_tombstones) {
    auto cloner = [] (const auto& x) {
        return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
    };
    _rows.clone_from(x._rows, cloner, current_deleter<rows_entry>());
}

mutation_partition::mutation_partition(const mutation_partition& x, const schema& schema,
        query::clustering_key_filter_ranges ck_ranges)
        : _tombstone(x._tombstone)
        , _static_row(x._static_row)
        , _rows(x._rows.value_comp())
        , _row_tombstones(x._row_tombstones) {
    try {
        for(auto&& r : ck_ranges) {
            for (const rows_entry& e : x.range(schema, r)) {
                std::unique_ptr<rows_entry> copy(current_allocator().construct<rows_entry>(e));
                _rows.insert(_rows.end(), *copy);
                copy.release();
            }
        }
    } catch (...) {
        _rows.clear_and_dispose(current_deleter<rows_entry>());
        throw;
    }
}

mutation_partition::mutation_partition(mutation_partition&& x, const schema& schema,
    query::clustering_key_filter_ranges ck_ranges)
    : _tombstone(x._tombstone)
    , _static_row(std::move(x._static_row))
    , _rows(std::move(x._rows))
    , _row_tombstones(std::move(x._row_tombstones))
{
    auto deleter = current_deleter<rows_entry>();
    if (ck_ranges.empty()) {
        _rows.clear_and_dispose(deleter);
        return;
    }

    auto range = ck_ranges.begin();
    auto it = _rows.begin();
    auto it_end = lower_bound(schema, *range);

    _rows.erase_and_dispose(it, it_end, deleter);

    auto next_range = std::next(range);
    while (next_range != ck_ranges.end()) {
        it = upper_bound(schema, *range);
        it_end = lower_bound(schema, *next_range);
        _rows.erase_and_dispose(it, it_end, deleter);

        range = next_range;
        next_range = std::next(range);
    }

    it = upper_bound(schema, *range);
    it_end = _rows.end();
    _rows.erase_and_dispose(it, it_end, deleter);
}

mutation_partition::~mutation_partition() {
    _rows.clear_and_dispose(current_deleter<rows_entry>());
}

mutation_partition&
mutation_partition::operator=(const mutation_partition& x) {
    mutation_partition n(x);
    std::swap(*this, n);
    return *this;
}

mutation_partition&
mutation_partition::operator=(mutation_partition&& x) noexcept {
    if (this != &x) {
        this->~mutation_partition();
        new (this) mutation_partition(std::move(x));
    }
    return *this;
}

void
mutation_partition::apply(const schema& s, const mutation_partition& p, const schema& p_schema) {
    if (s.version() != p_schema.version()) {
        auto p2 = p;
        p2.upgrade(p_schema, s);
        apply(s, std::move(p2));
        return;
    }

    mutation_partition tmp(p);
    apply(s, std::move(tmp));
}

void
mutation_partition::apply(const schema& s, mutation_partition&& p, const schema& p_schema) {
    if (s.version() != p_schema.version()) {
        // We can't upgrade p in-place due to exception guarantees
        apply(s, p, p_schema);
        return;
    }

    apply(s, std::move(p));
}

void
mutation_partition::apply(const schema& s, mutation_partition&& p) {
    auto revert_row_tombstones = _row_tombstones.apply_reversibly(s, p._row_tombstones);

    _static_row.apply_reversibly(s, column_kind::static_column, p._static_row);
    auto revert_static_row = defer([&] {
        _static_row.revert(s, column_kind::static_column, p._static_row);
    });

    auto revert_rows = apply_reversibly_intrusive_set(_rows, p._rows,
        [&s] (rows_entry& dst, rows_entry& src) { dst.apply_reversibly(s, src); },
        [&s] (rows_entry& dst, rows_entry& src) noexcept { dst.revert(s, src); });

    _tombstone.apply(p._tombstone); // noexcept

    revert_rows.cancel();
    revert_row_tombstones.cancel();
    revert_static_row.cancel();
}

void
mutation_partition::apply(const schema& s, mutation_partition_view p, const schema& p_schema) {
    if (p_schema.version() == s.version()) {
        mutation_partition p2(*this, copy_comparators_only{});
        partition_builder b(s, p2);
        p.accept(s, b);
        apply(s, std::move(p2));
    } else {
        mutation_partition p2(*this, copy_comparators_only{});
        partition_builder b(p_schema, p2);
        p.accept(p_schema, b);
        p2.upgrade(p_schema, s);
        apply(s, std::move(p2));
    }
}

tombstone
mutation_partition::range_tombstone_for_row(const schema& schema, const clustering_key& key) const {
    tombstone t = _tombstone;
    if (!_row_tombstones.empty()) {
        auto found = _row_tombstones.search_tombstone_covering(schema, key);
        t.apply(found);
    }
    return t;
}

tombstone
mutation_partition::tombstone_for_row(const schema& schema, const clustering_key& key) const {
    tombstone t = range_tombstone_for_row(schema, key);

    auto j = _rows.find(key, rows_entry::compare(schema));
    if (j != _rows.end()) {
        t.apply(j->row().deleted_at());
    }

    return t;
}

tombstone
mutation_partition::tombstone_for_row(const schema& schema, const rows_entry& e) const {
    tombstone t = range_tombstone_for_row(schema, e.key());
    t.apply(e.row().deleted_at());
    return t;
}

void
mutation_partition::apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t) {
    assert(!prefix.is_full(schema));
    auto start = prefix;
    _row_tombstones.apply(schema, {std::move(start), std::move(prefix), std::move(t)});
}

void
mutation_partition::apply_row_tombstone(const schema& schema, range_tombstone rt) {
    _row_tombstones.apply(schema, std::move(rt));
}

void
mutation_partition::apply_delete(const schema& schema, const exploded_clustering_prefix& prefix, tombstone t) {
    if (!prefix) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        apply_delete(schema, clustering_key::from_clustering_prefix(schema, prefix), t);
    } else {
        apply_row_tombstone(schema, clustering_key_prefix::from_clustering_prefix(schema, prefix), t);
    }
}

void
mutation_partition::apply_delete(const schema& schema, range_tombstone rt) {
    if (range_tombstone::is_single_clustering_row_tombstone(schema, rt.start, rt.start_kind, rt.end, rt.end_kind)) {
        apply_delete(schema, std::move(rt.start), std::move(rt.tomb));
        return;
    }
    apply_row_tombstone(schema, std::move(rt));
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key&& key, tombstone t) {
    clustered_row(schema, std::move(key)).apply(t);
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key_view key, tombstone t) {
    clustered_row(schema, key).apply(t);
}

void
mutation_partition::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at) {
    clustered_row(s, key).apply(created_at);
}

void mutation_partition::insert_row(const schema& s, const clustering_key& key, deletable_row&& row) {
    auto e = current_allocator().construct<rows_entry>(key, std::move(row));
    _rows.insert(_rows.end(), *e);
}

void mutation_partition::insert_row(const schema& s, const clustering_key& key, const deletable_row& row) {
    auto e = current_allocator().construct<rows_entry>(key, row);
    _rows.insert(_rows.end(), *e);
}

const row*
mutation_partition::find_row(const clustering_key& key) const {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells();
}

deletable_row&
mutation_partition::clustered_row(clustering_key&& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(std::move(key));
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const clustering_key& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(key);
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const schema& s, const clustering_key_view& key) {
    auto i = _rows.find(key, rows_entry::compare(s));
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(key);
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

mutation_partition::rows_type::const_iterator
mutation_partition::lower_bound(const schema& schema, const query::clustering_range& r) const {
    auto cmp = rows_entry::key_comparator(clustering_key_prefix::prefix_equality_less_compare(schema));
    return r.lower_bound(_rows, std::move(cmp));
}

mutation_partition::rows_type::const_iterator
mutation_partition::upper_bound(const schema& schema, const query::clustering_range& r) const {
    auto cmp = rows_entry::key_comparator(clustering_key_prefix::prefix_equality_less_compare(schema));
    return r.upper_bound(_rows, std::move(cmp));
}

boost::iterator_range<mutation_partition::rows_type::const_iterator>
mutation_partition::range(const schema& schema, const query::clustering_range& r) const {
    return boost::make_iterator_range(lower_bound(schema, r), upper_bound(schema, r));
}

template <typename Container>
boost::iterator_range<typename Container::iterator>
unconst(Container& c, boost::iterator_range<typename Container::const_iterator> r) {
    return boost::make_iterator_range(
        c.erase(r.begin(), r.begin()),
        c.erase(r.end(), r.end())
    );
}

template <typename Container>
typename Container::iterator
unconst(Container& c, typename Container::const_iterator i) {
    return c.erase(i, i);
}

boost::iterator_range<mutation_partition::rows_type::iterator>
mutation_partition::range(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->range(schema, r));
}

mutation_partition::rows_type::iterator
mutation_partition::lower_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->lower_bound(schema, r));
}

mutation_partition::rows_type::iterator
mutation_partition::upper_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->upper_bound(schema, r));
}

template<typename Func>
void mutation_partition::for_each_row(const schema& schema, const query::clustering_range& row_range, bool reversed, Func&& func) const
{
    auto r = range(schema, row_range);
    if (!reversed) {
        for (const auto& e : r) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    } else {
        for (const auto& e : r | boost::adaptors::reversed) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    }
}

template<typename RowWriter>
void write_cell(RowWriter& w, const query::partition_slice& slice, ::atomic_cell_view c) {
    assert(c.is_live());
    ser::writer_of_qr_cell wr = w.add().write();
    auto after_timestamp = [&, wr = std::move(wr)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_timestamp>()) {
            return std::move(wr).write_timestamp(c.timestamp());
        } else {
            return std::move(wr).skip_timestamp();
        }
    }();
    auto after_value = [&, wr = std::move(after_timestamp)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_expiry>() && c.is_live_and_has_ttl()) {
            return std::move(wr).write_expiry(c.expiry());
        } else {
            return std::move(wr).skip_expiry();
        }
    }().write_value(c.value());
    [&, wr = std::move(after_value)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_ttl>() && c.is_live_and_has_ttl()) {
            return std::move(wr).write_ttl(c.ttl());
        } else {
            return std::move(wr).skip_ttl();
        }
    }().end_qr_cell();
}

template<typename RowWriter>
void write_cell(RowWriter& w, const query::partition_slice& slice, const data_type& type, collection_mutation_view v) {
    auto ctype = static_pointer_cast<const collection_type_impl>(type);
    if (slice.options.contains<query::partition_slice::option::collections_as_maps>()) {
        ctype = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
    }
    w.add().write().skip_timestamp()
        .skip_expiry()
        .write_value(ctype->to_value(v, slice.cql_format()))
        .skip_ttl()
        .end_qr_cell();
}

// returns the timestamp of a latest update to the row
static api::timestamp_type hash_row_slice(md5_hasher& hasher,
    const schema& s,
    column_kind kind,
    const row& cells,
    const std::vector<column_id>& columns)
{
    api::timestamp_type max = api::missing_timestamp;
    for (auto id : columns) {
        const atomic_cell_or_collection* cell = cells.find_cell(id);
        if (!cell) {
            continue;
        }
        feed_hash(hasher, id);
        auto&& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            feed_hash(hasher, cell->as_atomic_cell());
            max = std::max(max, cell->as_atomic_cell().timestamp());
        } else {
            auto&& cm = cell->as_collection_mutation();
            feed_hash(hasher, cm);
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            max = std::max(max, ctype->last_update(cm));
        }
    }
    return max;
}

template<typename RowWriter>
static void get_compacted_row_slice(const schema& s,
    const query::partition_slice& slice,
    column_kind kind,
    const row& cells,
    const std::vector<column_id>& columns,
    RowWriter& writer)
{
    for (auto id : columns) {
        const atomic_cell_or_collection* cell = cells.find_cell(id);
        if (!cell) {
            writer.add().skip();
        } else {
            auto&& def = s.column_at(kind, id);
            if (def.is_atomic()) {
                auto c = cell->as_atomic_cell();
                if (!c.is_live()) {
                    writer.add().skip();
                } else {
                    write_cell(writer, slice, cell->as_atomic_cell());
                }
            } else {
                auto&& mut = cell->as_collection_mutation();
                auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
                if (!ctype->is_any_live(mut)) {
                    writer.add().skip();
                } else {
                    write_cell(writer, slice, def.type, mut);
                }
            }
        }
    }
}

bool has_any_live_data(const schema& s, column_kind kind, const row& cells, tombstone tomb = tombstone(),
                       gc_clock::time_point now = gc_clock::time_point::min()) {
    bool any_live = false;
    cells.for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& cell_or_collection) {
        const column_definition& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            auto&& c = cell_or_collection.as_atomic_cell();
            if (c.is_live(tomb, now)) {
                any_live = true;
                return stop_iteration::yes;
            }
        } else {
            auto&& cell = cell_or_collection.as_collection_mutation();
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            if (ctype->is_any_live(cell, tomb, now)) {
                any_live = true;
                return stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    });
    return any_live;
}

void
mutation_partition::query_compacted(query::result::partition_writer& pw, const schema& s, uint32_t limit) const {
    const query::partition_slice& slice = pw.slice();

    if (limit == 0) {
        pw.retract();
        return;
    }

    auto static_cells_wr = pw.start().start_static_row().start_cells();

    if (!slice.static_columns.empty()) {
        if (pw.requested_result()) {
            get_compacted_row_slice(s, slice, column_kind::static_column, static_row(), slice.static_columns, static_cells_wr);
        }
        if (pw.requested_digest()) {
            auto pt = partition_tombstone();
            ::feed_hash(pw.digest(), pt);
            auto t = hash_row_slice(pw.digest(), s, column_kind::static_column, static_row(), slice.static_columns);
            pw.last_modified() = std::max({pw.last_modified(), pt.timestamp, t});
        }
    }

    auto rows_wr = std::move(static_cells_wr).end_cells()
            .end_static_row()
            .start_rows();

    uint32_t row_count = 0;

    auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);
    auto send_ck = slice.options.contains(query::partition_slice::option::send_clustering_key);
    for_each_row(s, query::clustering_range::make_open_ended_both_sides(), is_reversed, [&] (const rows_entry& e) {
        auto& row = e.row();
        auto row_tombstone = tombstone_for_row(s, e);

        if (pw.requested_digest()) {
            e.key().feed_hash(pw.digest(), s);
            ::feed_hash(pw.digest(), row_tombstone);
            auto t = hash_row_slice(pw.digest(), s, column_kind::regular_column, row.cells(), slice.regular_columns);
            pw.last_modified() = std::max({pw.last_modified(), row_tombstone.timestamp, t});
        }

        if (row.is_live(s)) {
            if (pw.requested_result()) {
                auto cells_wr = [&] {
                    if (send_ck) {
                        return rows_wr.add().write_key(e.key()).start_cells().start_cells();
                    } else {
                        return rows_wr.add().skip_key().start_cells().start_cells();
                    }
                }();
                get_compacted_row_slice(s, slice, column_kind::regular_column, row.cells(), slice.regular_columns, cells_wr);
                std::move(cells_wr).end_cells().end_cells().end_qr_clustered_row();
            }
            ++row_count;
            if (--limit == 0) {
                return stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    });

    // If we got no rows, but have live static columns, we should only
    // give them back IFF we did not have any CK restrictions.
    // #589
    // If ck:s exist, and we do a restriction on them, we either have maching
    // rows, or return nothing, since cql does not allow "is null".
    if (row_count == 0
			&& (has_ck_selector(pw.ranges())
					|| !has_any_live_data(s, column_kind::static_column, static_row()))) {
		pw.retract();
	} else {
	    pw.row_count() += row_count ? : 1;
        pw.partition_count() += 1;
        std::move(rows_wr).end_rows().end_qr_partition();
	}
}

std::ostream&
operator<<(std::ostream& os, const std::pair<column_id, const atomic_cell_or_collection&>& c) {
    return fprint(os, "{column: %s %s}", c.first, c.second);
}

std::ostream&
operator<<(std::ostream& os, const row& r) {
    sstring cells;
    switch (r._type) {
    case row::storage_type::set:
        cells = ::join(", ", r.get_range_set());
        break;
    case row::storage_type::vector:
        cells = ::join(", ", r.get_range_vector());
        break;
    }
    return fprint(os, "{row: %s}", cells);
}

std::ostream&
operator<<(std::ostream& os, const row_marker& rm) {
    if (rm.is_missing()) {
        return fprint(os, "{missing row_marker}");
    } else if (rm._ttl == row_marker::dead) {
        return fprint(os, "{dead row_marker %s %s}", rm._timestamp, rm._expiry.time_since_epoch().count());
    } else {
        return fprint(os, "{row_marker %s %s %s}", rm._timestamp, rm._ttl.count(),
            rm._ttl != row_marker::no_ttl ? rm._expiry.time_since_epoch().count() : 0);
    }
}

std::ostream&
operator<<(std::ostream& os, const deletable_row& dr) {
    return fprint(os, "{deletable_row: %s %s %s}", dr._marker, dr._deleted_at, dr._cells);
}

std::ostream&
operator<<(std::ostream& os, const rows_entry& re) {
    return fprint(os, "{rows_entry: %s %s}", re._key, re._row);
}

std::ostream&
operator<<(std::ostream& os, const mutation_partition& mp) {
    return fprint(os, "{mutation_partition: %s (%s) static %s clustered %s}",
                  mp._tombstone, ::join(", ", mp._row_tombstones), mp._static_row,
                  ::join(", ", mp._rows));
}

constexpr gc_clock::duration row_marker::no_ttl;
constexpr gc_clock::duration row_marker::dead;

int compare_row_marker_for_merge(const row_marker& left, const row_marker& right) noexcept {
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() > right.timestamp() ? 1 : -1;
    }
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? -1 : 1;
    }
    if (left.is_live()) {
        if (left.is_expiring()
            && right.is_expiring()
            && left.expiry() != right.expiry())
        {
            return left.expiry() < right.expiry() ? -1 : 1;
        }
    } else {
        // Both are deleted
        if (left.deletion_time() != right.deletion_time()) {
            // Origin compares big-endian serialized deletion time. That's because it
            // delegates to AbstractCell.reconcile() which compares values after
            // comparing timestamps, which in case of deleted cells will hold
            // serialized expiry.
            return (uint32_t) left.deletion_time().time_since_epoch().count()
                   < (uint32_t) right.deletion_time().time_since_epoch().count() ? -1 : 1;
        }
    }
    return 0;
}

bool
deletable_row::equal(column_kind kind, const schema& s, const deletable_row& other, const schema& other_schema) const {
    if (_deleted_at != other._deleted_at || _marker != other._marker) {
        return false;
    }
    return _cells.equal(kind, s, other._cells, other_schema);
}

void deletable_row::apply_reversibly(const schema& s, deletable_row& src) {
    _cells.apply_reversibly(s, column_kind::regular_column, src._cells);
    _deleted_at.apply_reversibly(src._deleted_at); // noexcept
    _marker.apply_reversibly(src._marker); // noexcept
}

void deletable_row::revert(const schema& s, deletable_row& src) {
    _cells.revert(s, column_kind::regular_column, src._cells);
    _deleted_at.revert(src._deleted_at);
    _marker.revert(src._marker);
}

bool
rows_entry::equal(const schema& s, const rows_entry& other) const {
    return equal(s, other, s);
}

bool
rows_entry::equal(const schema& s, const rows_entry& other, const schema& other_schema) const {
    return key().equal(s, other.key()) // Only representation-compatible changes are allowed
           && row().equal(column_kind::regular_column, s, other.row(), other_schema);
}

bool mutation_partition::equal(const schema& s, const mutation_partition& p) const {
    return equal(s, p, s);
}

bool mutation_partition::equal(const schema& this_schema, const mutation_partition& p, const schema& p_schema) const {
    if (_tombstone != p._tombstone) {
        return false;
    }

    if (!std::equal(_rows.begin(), _rows.end(), p._rows.begin(), p._rows.end(),
        [&] (const rows_entry& e1, const rows_entry& e2) {
            return e1.equal(this_schema, e2, p_schema);
        }
    )) {
        return false;
    }

    if (!std::equal(_row_tombstones.begin(), _row_tombstones.end(),
        p._row_tombstones.begin(), p._row_tombstones.end(),
        [&] (const range_tombstone& rt1, const range_tombstone& rt2) { return rt1.equal(this_schema, rt2); }
    )) {
        return false;
    }

    return _static_row.equal(column_kind::static_column, this_schema, p._static_row, p_schema);
}

void
apply_reversibly(const column_definition& def, atomic_cell_or_collection& dst,  atomic_cell_or_collection& src) {
    // Must be run via with_linearized_managed_bytes() context, but assume it is
    // provided via an upper layer
    if (def.is_atomic()) {
        auto&& src_ac = src.as_atomic_cell_ref();
        if (compare_atomic_cell_for_merge(dst.as_atomic_cell(), src.as_atomic_cell()) < 0) {
            std::swap(dst, src);
            src_ac.set_revert(true);
        } else {
            src_ac.set_revert(false);
        }
    } else {
        auto ct = static_pointer_cast<const collection_type_impl>(def.type);
        src = ct->merge(dst.as_collection_mutation(), src.as_collection_mutation());
        std::swap(dst, src);
    }
}

void
revert(const column_definition& def, atomic_cell_or_collection& dst, atomic_cell_or_collection& src) noexcept {
    static_assert(std::is_nothrow_move_constructible<atomic_cell_or_collection>::value
                  && std::is_nothrow_move_assignable<atomic_cell_or_collection>::value,
                  "for std::swap() to be noexcept");
    if (def.is_atomic()) {
        auto&& ac = src.as_atomic_cell_ref();
        if (ac.is_revert_set()) {
            ac.set_revert(false);
            std::swap(dst, src);
        }
    } else {
        std::swap(dst, src);
    }
}

void
row::apply(const column_definition& column, const atomic_cell_or_collection& value) {
    atomic_cell_or_collection tmp(value);
    apply(column, std::move(tmp));
}

void
row::apply(const column_definition& column, atomic_cell_or_collection&& value) {
    apply_reversibly(column, value);
}

template<typename Func, typename Rollback>
void row::for_each_cell(Func&& func, Rollback&& rollback) {
    static_assert(noexcept(rollback(std::declval<column_id>(), std::declval<atomic_cell_or_collection&>())),
                           "rollback must be noexcept");

    if (_type == storage_type::vector) {
        unsigned i = 0;
        try {
            for (; i < _storage.vector.v.size(); i++) {
                if (_storage.vector.present.test(i)) {
                    func(i, _storage.vector.v[i]);
                }
            }
        } catch (...) {
            while (i) {
                --i;
                if (_storage.vector.present.test(i)) {
                    rollback(i, _storage.vector.v[i]);
                }
            }
            throw;
        }
    } else {
        auto i = _storage.set.begin();
        try {
            while (i != _storage.set.end()) {
                func(i->id(), i->cell());
                ++i;
            }
        } catch (...) {
            while (i != _storage.set.begin()) {
                --i;
                rollback(i->id(), i->cell());
            }
            throw;
        }
    }
}

template<typename Func>
void row::for_each_cell(Func&& func) {
    if (_type == storage_type::vector) {
        for (auto i : bitsets::for_each_set(_storage.vector.present)) {
            func(i, _storage.vector.v[i]);
        }
    } else {
        for (auto& cell : _storage.set) {
            func(cell.id(), cell.cell());
        }
    }
}

void
row::apply_reversibly(const column_definition& column, atomic_cell_or_collection& value) {
    static_assert(std::is_nothrow_move_constructible<atomic_cell_or_collection>::value
                  && std::is_nothrow_move_assignable<atomic_cell_or_collection>::value,
                  "noexcept required for atomicity");

    // our mutations are not yet immutable
    auto id = column.id;
    if (_type == storage_type::vector && id < max_vector_size) {
        if (id >= _storage.vector.v.size()) {
            _storage.vector.v.resize(id);
            _storage.vector.v.emplace_back(std::move(value));
            _storage.vector.present.set(id);
            _size++;
        } else if (!bool(_storage.vector.v[id])) {
            _storage.vector.v[id] = std::move(value);
            _storage.vector.present.set(id);
            _size++;
        } else {
            ::apply_reversibly(column, _storage.vector.v[id], value);
        }
    } else {
        if (_type == storage_type::vector) {
            vector_to_set();
        }
        auto i = _storage.set.lower_bound(id, cell_entry::compare());
        if (i == _storage.set.end() || i->id() != id) {
            cell_entry* e = current_allocator().construct<cell_entry>(id);
            std::swap(e->_cell, value);
            _storage.set.insert(i, *e);
            _size++;
        } else {
            ::apply_reversibly(column, i->cell(), value);
        }
    }
}

void
row::revert(const column_definition& column, atomic_cell_or_collection& src) noexcept {
    auto id = column.id;
    if (_type == storage_type::vector) {
        auto& dst = _storage.vector.v[id];
        if (!src) {
            std::swap(dst, src);
            _storage.vector.present.reset(id);
            --_size;
        } else {
            ::revert(column, dst, src);
        }
    } else {
        auto i = _storage.set.find(id, cell_entry::compare());
        auto& dst = i->cell();
        if (!src) {
            std::swap(dst, src);
            _storage.set.erase_and_dispose(i, current_deleter<cell_entry>());
            --_size;
        } else {
            ::revert(column, dst, src);
        }
    }
}

void
row::append_cell(column_id id, atomic_cell_or_collection value) {
    if (_type == storage_type::vector && id < max_vector_size) {
        _storage.vector.v.resize(id);
        _storage.vector.v.emplace_back(std::move(value));
        _storage.vector.present.set(id);
    } else {
        if (_type == storage_type::vector) {
            vector_to_set();
        }
        auto e = current_allocator().construct<cell_entry>(id, std::move(value));
        _storage.set.insert(_storage.set.end(), *e);
    }
    _size++;
}

const atomic_cell_or_collection*
row::find_cell(column_id id) const {
    if (_type == storage_type::vector) {
        if (id >= _storage.vector.v.size() || !_storage.vector.present.test(id)) {
            return nullptr;
        }
        return &_storage.vector.v[id];
    } else {
        auto i = _storage.set.find(id, cell_entry::compare());
        if (i == _storage.set.end()) {
            return nullptr;
        }
        return &i->cell();
    }
}

size_t row::external_memory_usage() const {
    size_t mem = 0;
    if (_type == storage_type::vector) {
        mem += _storage.vector.v.external_memory_usage();
        for (auto&& ac_o_c : _storage.vector.v) {
            mem += ac_o_c.external_memory_usage();
        }
    } else {
        for (auto&& ce : _storage.set) {
            mem += sizeof(cell_entry) + ce.cell().external_memory_usage();
        }
    }
    return mem;
}

template<bool reversed, typename Func>
void mutation_partition::trim_rows(const schema& s,
    const std::vector<query::clustering_range>& row_ranges,
    Func&& func)
{
    static_assert(std::is_same<stop_iteration, std::result_of_t<Func(rows_entry&)>>::value, "Bad func signature");

    bool stop = false;
    auto last = reversal_traits<reversed>::begin(_rows);
    auto deleter = current_deleter<rows_entry>();

    auto range_begin = [this, &s] (const query::clustering_range& range) {
        return reversed ? upper_bound(s, range) : lower_bound(s, range);
    };

    auto range_end = [this, &s] (const query::clustering_range& range) {
        return reversed ? lower_bound(s, range) : upper_bound(s, range);
    };

    for (auto&& row_range : row_ranges) {
        if (stop) {
            break;
        }

        last = reversal_traits<reversed>::erase_and_dispose(_rows, last,
            reversal_traits<reversed>::maybe_reverse(_rows, range_begin(row_range)), deleter);

        auto end = reversal_traits<reversed>::maybe_reverse(_rows, range_end(row_range));
        while (last != end) {
            rows_entry& e = *last;
            if (func(e) == stop_iteration::yes) {
                stop = true;
                break;
            }

            if (e.empty()) {
                last = reversal_traits<reversed>::erase_dispose_and_update_end(_rows, last, deleter, end);
            } else {
                ++last;
            }
        }
    }

    reversal_traits<reversed>::erase_and_dispose(_rows, last, reversal_traits<reversed>::end(_rows), deleter);
}

uint32_t mutation_partition::do_compact(const schema& s,
    gc_clock::time_point query_time,
    const std::vector<query::clustering_range>& row_ranges,
    bool reverse,
    uint32_t row_limit,
    can_gc_fn& can_gc)
{
    assert(row_limit > 0);

    auto gc_before = query_time - s.gc_grace_seconds();

    auto should_purge_tombstone = [&] (const tombstone& t) {
        return t.deletion_time < gc_before && can_gc(t);
    };

    bool static_row_live = _static_row.compact_and_expire(s, column_kind::static_column, _tombstone,
        query_time, can_gc, gc_before);

    uint32_t row_count = 0;

    auto row_callback = [&] (rows_entry& e) {
        deletable_row& row = e.row();

        tombstone tomb = tombstone_for_row(s, e);

        bool is_live = row.cells().compact_and_expire(s, column_kind::regular_column, tomb, query_time, can_gc, gc_before);
        is_live |= row.marker().compact_and_expire(tomb, query_time, can_gc, gc_before);

        if (should_purge_tombstone(row.deleted_at())) {
            row.remove_tombstone();
        }

        // when row_limit is reached, do not exit immediately,
        // iterate to the next live_row instead to include trailing
        // tombstones in the mutation. This is how Origin deals with
        // https://issues.apache.org/jira/browse/CASSANDRA-8933
        if (is_live) {
            if (row_count == row_limit) {
                return stop_iteration::yes;
            }
            ++row_count;
        }

        return stop_iteration::no;
    };

    if (reverse) {
        trim_rows<true>(s, row_ranges, row_callback);
    } else {
        trim_rows<false>(s, row_ranges, row_callback);
    }

    // #589 - Do not add extra row for statics unless we did a CK range-less query.
    // See comment in query
    if (row_count == 0 && static_row_live && !has_ck_selector(row_ranges)) {
        ++row_count;
    }

    _row_tombstones.erase_where([&] (auto&& rt) {
        return should_purge_tombstone(rt.tomb) || rt.tomb.timestamp <= _tombstone.timestamp;
    });
    if (should_purge_tombstone(_tombstone)) {
        _tombstone = tombstone();
    }

    // FIXME: purge unneeded prefix tombstones based on row_ranges

    return row_count;
}

uint32_t
mutation_partition::compact_for_query(
    const schema& s,
    gc_clock::time_point query_time,
    const std::vector<query::clustering_range>& row_ranges,
    bool reverse,
    uint32_t row_limit)
{
    return do_compact(s, query_time, row_ranges, reverse, row_limit, always_gc);
}

void mutation_partition::compact_for_compaction(const schema& s,
    can_gc_fn& can_gc, gc_clock::time_point compaction_time)
{
    static const std::vector<query::clustering_range> all_rows = {
        query::clustering_range::make_open_ended_both_sides()
    };

    do_compact(s, compaction_time, all_rows, false, query::max_rows, can_gc);
}

// Returns true if there is no live data or tombstones.
bool mutation_partition::empty() const
{
    if (_tombstone.timestamp != api::missing_timestamp) {
        return false;
    }
    return !_static_row.size() && _rows.empty() && _row_tombstones.empty();
}

bool
deletable_row::is_live(const schema& s, tombstone base_tombstone, gc_clock::time_point query_time) const {
    // _created_at corresponds to the row marker cell, present for rows
    // created with the 'insert' statement. If row marker is live, we know the
    // row is live. Otherwise, a row is considered live if it has any cell
    // which is live.
    base_tombstone.apply(_deleted_at);
    return _marker.is_live(base_tombstone, query_time)
           || has_any_live_data(s, column_kind::regular_column, _cells, base_tombstone, query_time);
}

bool
mutation_partition::is_static_row_live(const schema& s, gc_clock::time_point query_time) const {
    return has_any_live_data(s, column_kind::static_column, static_row(), _tombstone, query_time);
}

size_t
mutation_partition::live_row_count(const schema& s, gc_clock::time_point query_time) const {
    size_t count = 0;

    for (const rows_entry& e : _rows) {
        tombstone base_tombstone = range_tombstone_for_row(s, e.key());
        if (e.row().is_live(s, base_tombstone, query_time)) {
            ++count;
        }
    }

    if (count == 0 && is_static_row_live(s, query_time)) {
        return 1;
    }

    return count;
}

rows_entry::rows_entry(rows_entry&& o) noexcept
    : _key(std::move(o._key))
    , _row(std::move(o._row))
{
    using container_type = mutation_partition::rows_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

row::row(const row& o)
    : _type(o._type)
    , _size(o._size)
{
    if (_type == storage_type::vector) {
        new (&_storage.vector) vector_storage(o._storage.vector);
    } else {
        auto cloner = [] (const auto& x) {
            return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
        };
        new (&_storage.set) map_type;
        try {
            _storage.set.clone_from(o._storage.set, cloner, current_deleter<cell_entry>());
        } catch (...) {
            _storage.set.~map_type();
            throw;
        }
    }
}

row::~row() {
    if (_type == storage_type::vector) {
        _storage.vector.~vector_storage();
    } else {
        _storage.set.clear_and_dispose(current_deleter<cell_entry>());
        _storage.set.~map_type();
    }
}

row::cell_entry::cell_entry(const cell_entry& o)
    : _id(o._id)
    , _cell(o._cell)
{ }

row::cell_entry::cell_entry(cell_entry&& o) noexcept
    : _link()
    , _id(o._id)
    , _cell(std::move(o._cell))
{
    using container_type = row::map_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

const atomic_cell_or_collection& row::cell_at(column_id id) const {
    auto&& cell = find_cell(id);
    if (!cell) {
        throw std::out_of_range(sprint("Column not found for id = %d", id));
    }
    return *cell;
}

void row::vector_to_set()
{
    assert(_type == storage_type::vector);
    map_type set;
    try {
    for (auto i : bitsets::for_each_set(_storage.vector.present)) {
        auto& c = _storage.vector.v[i];
        auto e = current_allocator().construct<cell_entry>(i, std::move(c));
        set.insert(set.end(), *e);
    }
    } catch (...) {
        set.clear_and_dispose([this, del = current_deleter<cell_entry>()] (cell_entry* ce) noexcept {
            _storage.vector.v[ce->id()] = std::move(ce->cell());
            del(ce);
        });
        throw;
    }
    _storage.vector.~vector_storage();
    new (&_storage.set) map_type(std::move(set));
    _type = storage_type::set;
}

void row::reserve(column_id last_column)
{
    if (_type == storage_type::vector && last_column >= internal_count) {
        if (last_column >= max_vector_size) {
            vector_to_set();
        } else {
            _storage.vector.v.reserve(last_column);
        }
    }
}

template<typename Func>
auto row::with_both_ranges(const row& other, Func&& func) const {
    if (_type == storage_type::vector) {
        if (other._type == storage_type::vector) {
            return func(get_range_vector(), other.get_range_vector());
        } else {
            return func(get_range_vector(), other.get_range_set());
        }
    } else {
        if (other._type == storage_type::vector) {
            return func(get_range_set(), other.get_range_vector());
        } else {
            return func(get_range_set(), other.get_range_set());
        }
    }
}

bool row::operator==(const row& other) const {
    if (size() != other.size()) {
        return false;
    }

    auto cells_equal = [] (std::pair<column_id, const atomic_cell_or_collection&> c1,
                           std::pair<column_id, const atomic_cell_or_collection&> c2) {
        return c1.first == c2.first && c1.second == c2.second;
    };
    return with_both_ranges(other, [&] (auto r1, auto r2) {
        return boost::equal(r1, r2, cells_equal);
    });
}

bool row::equal(column_kind kind, const schema& this_schema, const row& other, const schema& other_schema) const {
    if (size() != other.size()) {
        return false;
    }

    auto cells_equal = [&] (std::pair<column_id, const atomic_cell_or_collection&> c1,
                            std::pair<column_id, const atomic_cell_or_collection&> c2) {
        static_assert(schema::row_column_ids_are_ordered_by_name::value, "Relying on column ids being ordered by name");
        return this_schema.column_at(kind, c1.first).name() == other_schema.column_at(kind, c2.first).name()
               && c1.second == c2.second;
    };
    return with_both_ranges(other, [&] (auto r1, auto r2) {
        return boost::equal(r1, r2, cells_equal);
    });
}

row::row() {
    new (&_storage.vector) vector_storage;
}

row::row(row&& other) noexcept
    : _type(other._type), _size(other._size) {
    if (_type == storage_type::vector) {
        new (&_storage.vector) vector_storage(std::move(other._storage.vector));
    } else {
        new (&_storage.set) map_type(std::move(other._storage.set));
    }
}

row& row::operator=(row&& other) noexcept {
    if (this != &other) {
        this->~row();
        new (this) row(std::move(other));
    }
    return *this;
}

void row::apply_reversibly(const schema& s, column_kind kind, row& other) {
    if (other.empty()) {
        return;
    }
    if (other._type == storage_type::vector) {
        reserve(other._storage.vector.v.size() - 1);
    } else {
        reserve(other._storage.set.rbegin()->id());
    }
    other.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) {
        apply_reversibly(s.column_at(kind, id), cell);
    }, [&] (column_id id, atomic_cell_or_collection& cell) noexcept {
        revert(s.column_at(kind, id), cell);
    });
}

void row::apply(const schema& s, column_kind kind, const row& other) {
    if (other.empty()) {
        return;
    }
    if (other._type == storage_type::vector) {
        reserve(other._storage.vector.v.size() - 1);
    } else {
        reserve(other._storage.set.rbegin()->id());
    }
    other.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        apply(s.column_at(kind, id), cell);
    });
}

void row::apply(const schema& s, column_kind kind, row&& other) {
    if (other.empty()) {
        return;
    }
    if (other._type == storage_type::vector) {
        reserve(other._storage.vector.v.size() - 1);
    } else {
        reserve(other._storage.set.rbegin()->id());
    }
    other.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) {
        apply(s.column_at(kind, id), std::move(cell));
    });
}

void row::revert(const schema& s, column_kind kind, row& other) noexcept {
    other.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) noexcept {
        revert(s.column_at(kind, id), cell);
    });
}

bool row::compact_and_expire(const schema& s, column_kind kind, tombstone tomb, gc_clock::time_point query_time,
    can_gc_fn& can_gc, gc_clock::time_point gc_before)
{
    bool any_live = false;
    remove_if([&] (column_id id, atomic_cell_or_collection& c) {
        bool erase = false;
        const column_definition& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            atomic_cell_view cell = c.as_atomic_cell();
            if (cell.is_covered_by(tomb)) {
                erase = true;
            } else if (cell.has_expired(query_time)) {
                c = atomic_cell::make_dead(cell.timestamp(), cell.deletion_time());
            } else if (!cell.is_live()) {
                erase = cell.deletion_time() < gc_before && can_gc(tombstone(cell.timestamp(), cell.deletion_time()));
            } else {
                any_live |= true;
            }
        } else {
            auto&& cell = c.as_collection_mutation();
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            auto m_view = ctype->deserialize_mutation_form(cell);
            collection_type_impl::mutation m = m_view.materialize();
            any_live |= m.compact_and_expire(tomb, query_time, can_gc, gc_before);
            if (m.cells.empty() && m.tomb <= tomb) {
                erase = true;
            } else {
                c = ctype->serialize_mutation_form(m);
            }
        }
        return erase;
    });
    return any_live;
}

deletable_row deletable_row::difference(const schema& s, column_kind kind, const deletable_row& other) const
{
    deletable_row dr;
    if (_deleted_at > other._deleted_at) {
        dr.apply(_deleted_at);
    }
    if (compare_row_marker_for_merge(_marker, other._marker) > 0) {
        dr.apply(_marker);
    }
    dr._cells = _cells.difference(s, kind, other._cells);
    return dr;
}

row row::difference(const schema& s, column_kind kind, const row& other) const
{
    row r;
    with_both_ranges(other, [&] (auto this_range, auto other_range) {
        auto it = other_range.begin();
        for (auto&& c : this_range) {
            while (it != other_range.end() && it->first < c.first) {
                ++it;
            }
            if (it == other_range.end() || it->first != c.first) {
                r.append_cell(c.first, c.second);
            } else if (s.column_at(kind, c.first).is_atomic()) {
                if (compare_atomic_cell_for_merge(c.second.as_atomic_cell(), it->second.as_atomic_cell()) > 0) {
                    r.append_cell(c.first, c.second);
                }
            } else {
                auto ct = static_pointer_cast<const collection_type_impl>(s.column_at(kind, c.first).type);
                auto diff = ct->difference(c.second.as_collection_mutation(), it->second.as_collection_mutation());
                if (!ct->is_empty(diff)) {
                    r.append_cell(c.first, std::move(diff));
                }
            }
        }
    });
    return r;
}

mutation_partition mutation_partition::difference(schema_ptr s, const mutation_partition& other) const
{
    mutation_partition mp(s);
    if (_tombstone > other._tombstone) {
        mp.apply(_tombstone);
    }
    mp._static_row = _static_row.difference(*s, column_kind::static_column, other._static_row);

    mp._row_tombstones = _row_tombstones.difference(*s, other._row_tombstones);

    auto it_r = other._rows.begin();
    rows_entry::compare cmp_r(*s);
    for (auto&& r : _rows) {
        while (it_r != other._rows.end() && cmp_r(*it_r, r)) {
            ++it_r;
        }
        if (it_r == other._rows.end() || !it_r->key().equal(*s, r.key())) {
            mp.insert_row(*s, r.key(), r.row());
        } else {
            auto dr = r.row().difference(*s, column_kind::regular_column, it_r->row());
            if (!dr.empty()) {
                mp.insert_row(*s, r.key(), std::move(dr));
            }
        }
    }
    return mp;
}

void mutation_partition::accept(const schema& s, mutation_partition_visitor& v) const {
    v.accept_partition_tombstone(_tombstone);
    _static_row.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        const column_definition& def = s.static_column_at(id);
        if (def.is_atomic()) {
            v.accept_static_cell(id, cell.as_atomic_cell());
        } else {
            v.accept_static_cell(id, cell.as_collection_mutation());
        }
    });
    for (const range_tombstone& rt : _row_tombstones) {
        v.accept_row_tombstone(rt);
    }
    for (const rows_entry& e : _rows) {
        const deletable_row& dr = e.row();
        v.accept_row(e.key(), dr.deleted_at(), dr.marker());
        dr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            const column_definition& def = s.regular_column_at(id);
            if (def.is_atomic()) {
                v.accept_row_cell(id, cell.as_atomic_cell());
            } else {
                v.accept_row_cell(id, cell.as_collection_mutation());
            }
        });
    }
}

void
mutation_partition::upgrade(const schema& old_schema, const schema& new_schema) {
    // We need to copy to provide strong exception guarantees.
    mutation_partition tmp(new_schema.shared_from_this());
    converting_mutation_partition_applier v(old_schema.get_column_mapping(), new_schema, tmp);
    accept(old_schema, v);
    *this = std::move(tmp);
}

void row_marker::apply_reversibly(row_marker& rm) noexcept {
    if (compare_row_marker_for_merge(*this, rm) < 0) {
        std::swap(*this, rm);
    } else {
        rm = *this;
    }
}

void row_marker::revert(row_marker& rm) noexcept {
    std::swap(*this, rm);
}

// Adds mutation to query::result.
class mutation_querier {
    const schema& _schema;
    query::result::partition_writer& _pw;
    ser::qr_partition__static_row__cells _static_cells_wr;
    bool _live_data_in_static_row{};
    uint32_t _live_clustering_rows = 0;
    stdx::optional<ser::qr_partition__rows> _rows_wr;
private:
    void query_static_row(const row& r, tombstone current_tombstone);
    void prepare_writers();
public:
    mutation_querier(const schema& s, query::result::partition_writer& pw);
    void consume(tombstone) { }
    // Requires that sr.has_any_live_data()
    stop_iteration consume(static_row&& sr, tombstone current_tombstone);
    // Requires that cr.has_any_live_data()
    stop_iteration consume(clustering_row&& cr, tombstone current_tombstone);
    stop_iteration consume(range_tombstone&&) { return stop_iteration::no; }
    void consume_end_of_stream();
};

mutation_querier::mutation_querier(const schema& s, query::result::partition_writer& pw)
    : _schema(s)
    , _pw(pw)
    , _static_cells_wr(pw.start().start_static_row().start_cells())
{
}

void mutation_querier::query_static_row(const row& r, tombstone current_tombstone)
{
    const query::partition_slice& slice = _pw.slice();
    if (!slice.static_columns.empty()) {
        if (_pw.requested_result()) {
            get_compacted_row_slice(_schema, slice, column_kind::static_column,
                                    r, slice.static_columns, _static_cells_wr);
        }
        if (_pw.requested_digest()) {
            ::feed_hash(_pw.digest(), current_tombstone);
            auto t = hash_row_slice(_pw.digest(), _schema, column_kind::static_column,
                                    r, slice.static_columns);
            _pw.last_modified() = std::max({_pw.last_modified(), current_tombstone.timestamp, t});
        }
    }
    _rows_wr.emplace(std::move(_static_cells_wr).end_cells().end_static_row().start_rows());
}

stop_iteration mutation_querier::consume(static_row&& sr, tombstone current_tombstone) {
    query_static_row(sr.cells(), current_tombstone);
    _live_data_in_static_row = true;
    return stop_iteration::no;
}

void mutation_querier::prepare_writers() {
    if (!_rows_wr) {
        row empty_row;
        query_static_row(empty_row, { });
        _live_data_in_static_row = false;
    }
}

stop_iteration mutation_querier::consume(clustering_row&& cr, tombstone current_tombstone) {
    prepare_writers();

    const query::partition_slice& slice = _pw.slice();

    if (_pw.requested_digest()) {
        cr.key().feed_hash(_pw.digest(), _schema);
        ::feed_hash(_pw.digest(), current_tombstone);
        auto t = hash_row_slice(_pw.digest(), _schema, column_kind::regular_column, cr.cells(), slice.regular_columns);
        _pw.last_modified() = std::max({_pw.last_modified(), current_tombstone.timestamp, t});
    }

    if (_pw.requested_result()) {
        auto cells_wr = [&] {
            if (slice.options.contains(query::partition_slice::option::send_clustering_key)) {
                return _rows_wr->add().write_key(cr.key()).start_cells().start_cells();
            } else {
                return _rows_wr->add().skip_key().start_cells().start_cells();
            }
        }();
        get_compacted_row_slice(_schema, slice, column_kind::regular_column, cr.cells(), slice.regular_columns, cells_wr);
        std::move(cells_wr).end_cells().end_cells().end_qr_clustered_row();
    }

    _live_clustering_rows++;
    return stop_iteration::no;
}

void mutation_querier::consume_end_of_stream() {
    prepare_writers();

    // If we got no rows, but have live static columns, we should only
    // give them back IFF we did not have any CK restrictions.
    // #589
    // If ck:s exist, and we do a restriction on them, we either have maching
    // rows, or return nothing, since cql does not allow "is null".
    if (!_live_clustering_rows
        && (has_ck_selector(_pw.ranges()) || !_live_data_in_static_row)) {
        _pw.retract();
    } else {
        _pw.row_count() += std::max(_live_clustering_rows, uint32_t(1));
        _pw.partition_count() += 1;
        std::move(*_rows_wr).end_rows().end_qr_partition();
    }
}

class query_result_builder {
    const schema& _schema;
    query::result::builder& _rb;
    stdx::optional<query::result::partition_writer> _pw;
    stdx::optional<mutation_querier> _mutation_consumer;
public:
    query_result_builder(const schema& s, query::result::builder& rb)
            : _schema(s), _rb(rb) { }

    void consume_new_partition(const dht::decorated_key& dk) {
        _pw.emplace(_rb.add_partition(_schema, dk.key()));
        _mutation_consumer.emplace(mutation_querier(_schema, *_pw));
    }

    void consume(tombstone t) {
        _mutation_consumer->consume(t);
    }
    void consume(static_row&& sr, tombstone t, bool) {
        _mutation_consumer->consume(std::move(sr), t);
    }
    void consume(clustering_row&& cr, tombstone t,  bool) {
        _mutation_consumer->consume(std::move(cr), t);
    }
    void consume(range_tombstone&& rt) {
        _mutation_consumer->consume(std::move(rt));
    }

    void consume_end_of_partition() {
        _mutation_consumer->consume_end_of_stream();
    }

    data_query_result consume_end_of_stream() {
        return {_rb.row_count(), _rb.partition_count()};
    }
};

future<data_query_result> data_query(schema_ptr s, const mutation_source& source, const query::partition_range& range,
                            const query::partition_slice& slice, uint32_t row_limit, uint32_t partition_limit,
                            gc_clock::time_point query_time, query::result::builder& builder)
{
    if (row_limit == 0 || slice.partition_row_limit() == 0 || partition_limit == 0) {
        return make_ready_future<data_query_result>();
    }

    auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);

    auto qrb = query_result_builder(*s, builder);
    auto cfq = make_stable_flattened_mutations_consumer<compact_for_query<emit_only_live_rows::yes, query_result_builder>>(
            *s, query_time, slice, row_limit, partition_limit, std::move(qrb));

    auto reader = source(s, range, slice, service::get_local_sstable_query_read_priority());
    return consume_flattened(std::move(reader), std::move(cfq), is_reversed);
}

class reconcilable_result_builder {
    const schema& _schema;
    const query::partition_slice& _slice;

    std::vector<partition> _result;
    uint32_t _live_rows;

    bool _has_ck_selector{};
    bool _static_row_is_alive{};
    uint32_t _total_live_rows = 0;
    stdx::optional<streamed_mutation_freezer> _mutation_consumer;
public:
    reconcilable_result_builder(const schema& s, const query::partition_slice& slice)
            : _schema(s), _slice(slice) { }

    void consume_new_partition(const dht::decorated_key& dk) {
        _has_ck_selector = has_ck_selector(_slice.row_ranges(_schema, dk.key()));
        _static_row_is_alive = false;
        _live_rows = 0;
        auto is_reversed = _slice.options.contains(query::partition_slice::option::reversed);
        _mutation_consumer.emplace(streamed_mutation_freezer(_schema, dk.key(), is_reversed));
    }

    void consume(tombstone t) {
        _mutation_consumer->consume(t);
    }
    void consume(static_row&& sr, tombstone, bool is_alive) {
        _static_row_is_alive = is_alive;
        _mutation_consumer->consume(std::move(sr));
    }
    void consume(clustering_row&& cr, tombstone, bool is_alive) {
        _live_rows += is_alive;
        _mutation_consumer->consume(std::move(cr));
    }
    void consume(range_tombstone&& rt) {
        _mutation_consumer->consume(std::move(rt));
    }

    void consume_end_of_partition() {
        if (_live_rows == 0 && _static_row_is_alive && !_has_ck_selector) {
            ++_live_rows;
        }
        _total_live_rows += _live_rows;
        _result.emplace_back(partition { _live_rows, _mutation_consumer->consume_end_of_stream() });
    }

    reconcilable_result consume_end_of_stream() {
        return reconcilable_result(_total_live_rows, std::move(_result));
    }
};

future<reconcilable_result>
mutation_query(schema_ptr s,
               const mutation_source& source,
               const query::partition_range& range,
               const query::partition_slice& slice,
               uint32_t row_limit,
               uint32_t partition_limit,
               gc_clock::time_point query_time)
{
    if (row_limit == 0 || slice.partition_row_limit() == 0 || partition_limit == 0) {
        return make_ready_future<reconcilable_result>(reconcilable_result());
    }

    auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);

    auto rrb = reconcilable_result_builder(*s, slice);
    auto cfq = make_stable_flattened_mutations_consumer<compact_for_query<emit_only_live_rows::no, reconcilable_result_builder>>(
            *s, query_time, slice, row_limit, partition_limit, std::move(rrb));

    auto reader = source(s, range, slice, service::get_local_sstable_query_read_priority());
    return consume_flattened(std::move(reader), std::move(cfq), is_reversed);
}
