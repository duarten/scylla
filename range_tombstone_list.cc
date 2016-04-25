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

#include "range_tombstone_list.hh"
#include "utils/allocation_strategy.hh"

range_tombstone_list::range_tombstone_list(const range_tombstone_list& x)
        : _tombstones(x._tombstones.value_comp()) {
    auto cloner = [] (const auto& x) {
        return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
    };
    try {
        _tombstones.clone_from(x._tombstones, cloner, current_deleter<range_tombstone>());
    } catch (...) {
        _tombstones.clear_and_dispose(current_deleter<range_tombstone>());
        throw;
    }
}

range_tombstone_list::~range_tombstone_list() {
    _tombstones.clear_and_dispose(current_deleter<range_tombstone>());
}

void range_tombstone_list::add(const schema& s, clustering_key_prefix start, clustering_key_prefix stop, tombstone tomb) {
    if (_tombstones.size() > 0) {
        clustering_key_prefix::less_compare less(s);
        auto last = --_tombstones.end();
        if (!less(last->stop, start)) { // if (last->stop >= start)
            auto it = _tombstones.lower_bound(start, range_tombstone::compare(s));
            insert_from(s, std::move(it), std::move(start), std::move(stop), std::move(tomb));
            return;
        }
    }
    auto rt = current_allocator().construct<range_tombstone>(std::move(start), std::move(stop), std::move(tomb));
    _tombstones.push_back(*rt);
}

/*
 * Inserts a new element starting at the position pointed to by the iterator, it.
 * This method assumes that:
 *    (it - 1)->stop <= start <= it->stop
 *
 * A range tombstone list is a list of ranges [s_0, e_0]...[s_n, e_n] such that:
 *   - s_i <= e_i
 *   - e_i <= s_i+1
 *   - if s_i == e_i and e_i == s_i+1 then s_i+1 < e_i+1
 * Basically, ranges are ordered and non-overlapping, except for their bounds: we
 * allow ranges with the same value for the start and stop keys, but we don't allow
 * repeating such range (e.g., we don't allow [0, 0][0, 0]).
 */
void range_tombstone_list::insert_from(const schema& s,
    range_tombstones_type::iterator it,
    clustering_key_prefix start,
    clustering_key_prefix stop,
    tombstone tomb)
{
    clustering_key_prefix::less_compare less(s);

    while (it != _tombstones.end()) {
        if (start.equal(s, it->stop)) {
            // The new tombstone really starts at the next one, except for the case where
            // it->start == it->stop. In that case, if we were to move to the next tombstone,
            // we could end up with ...[x, x][x, x]...].
            if (it->start.equal(s, it->stop)) {
                if (tomb.timestamp > it->tomb.timestamp) {
                    // The new tombstone overwrites the current one, so remove it and proceed
                    // with the insert.
                    it = _tombstones.erase_and_dispose(it, current_deleter<range_tombstone>());
                    continue;
                }

                // The current singleton range overrides the new one. If the new tombstone is
                // also a singleton, then it is fully covered and we return.
                if (start.equal(s, stop)) {
                    return;
                }
            }
            ++it;
            continue;
        }

        if (tomb.timestamp > it->tomb.timestamp) {
            // We overwrite the current tombstone.

            if (less(it->start, start)) {
                auto rt = current_allocator().construct<range_tombstone>(it->start, start, it->tomb);
                it = _tombstones.insert_before(it, *rt);
                ++it;
                // We don't need to execute the following line, but it helps readability:
                // *it = {start, it->stop, it->tomb};
            }

            // Here start <= it->start.

            if (less(stop, it->start)) {
                // Here start <= it->start and stop < it->start, so the new tombstone is
                // before the current one.
                auto rt = current_allocator().construct<range_tombstone>(std::move(start), std::move(stop), std::move(tomb));
                it = _tombstones.insert_before(it, *rt);
                return;
            }

            if (stop.equal(s, it->start) && it->start.equal(s, it->stop)) {
                // Here the new tombstone entirely overwrites the current one.
                *it = {std::move(start), std::move(stop), std::move(tomb)};
                return;
            }

            if (less(stop, it->stop)) {
                // Here start <= it->start and stop < it->stop.
                auto rt = current_allocator().construct<range_tombstone>(std::move(start), stop, std::move(tomb));
                it = _tombstones.insert_before(it, *rt);
                ++it;
                *it = {std::move(stop), it->stop, it->tomb};
                return;
            }

            // Here start <= it->start and stop >= it->stop.

            // If we're on the last tombstone, or if we stop before the next start, we set the
            // new tombstone and are done.
            auto next = range_tombstones_type::node_algorithms::next_node(it.pointed_node());
			if (next == _tombstones.end().pointed_node()
                    || !less(range_tombstones_type::value_traits::to_value_ptr(next)->start, stop)) {
				*it = {start, stop, tomb};
				return;
 			}

            *it = {start, it->stop, tomb};
            if (stop.equal(s, it->stop)) {
                return;
            }
            // Continue with the new range, it->stop to stop.
            start = it->stop;
            ++it;
        } else {
            // We don't overwrite the current tombstone.

            if (less(start, it->start)) {
                // The new tombstone starts before the current one.
                if (less(it->start, stop)) {
                    // Here start < it->start and it->start < stop.
                    auto rt = current_allocator().construct<range_tombstone>(std::move(start), it->start, tomb);
                    it = _tombstones.insert_before(it, *rt);
                    ++it;
                } else {
                    // Here start < it->start and stop <= it->start, so just insert the new tombstone.
                    auto rt = current_allocator().construct<range_tombstone>(std::move(start), std::move(stop), std::move(tomb));
                    _tombstones.insert_before(it, *rt);
                   return;
                }
            }

            if (less(it->stop, stop)) {
                // Here, the current tombstone overwrites a range of the new one.
                start = it->stop;
                ++it;
            } else {
                // Here, the current tombstone completely overwrites the new one.
                return;
            }
        }
    }

    // If we got here, then just insert the remainder at the end.
    auto rt = current_allocator().construct<range_tombstone>(std::move(start), std::move(stop), std::move(tomb));
    _tombstones.push_back(*rt);
}

/*
 * Returns the tombstone covering the specified key, or an empty tombstone otherwise.
 */
tombstone range_tombstone_list::search_tombstone_covering(const schema& s, const clustering_key& key) const {
    clustering_key_prefix_view::less_compare less(s);

    auto it = _tombstones.lower_bound(key, range_tombstone::compare(s));
    if (it == _tombstones.end() || less(key, it->start)) {
        return {};
    }

    auto next = range_tombstones_type::node_algorithms::next_node(it.pointed_node());
    if (next != _tombstones.end().pointed_node()) {
        auto next_rt = range_tombstones_type::value_traits::to_value_ptr(next);
        if (!less(key, next_rt->start) && next_rt->tomb > it->tomb) {
            return next_rt->tomb;
        }
    }

    return it->tomb;
}
