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

#pragma once

#include "range_tombstone.hh"
#include "exceptions/exceptions.hh"

/**
 * Class used to transform a set of range_tombstones into a set of
 * overlapping tombstones, in order to support sending mutations to
 * nodes that don't yet support range tombstones.
 */
class range_tombstone_merger {
    class open_tombstone {
    public:
        clustering_key_prefix prefix;
        tombstone tomb;
        open_tombstone(clustering_key_prefix start, tombstone tomb)
                : prefix(std::move(start))
                , tomb(std::move(tomb))
        { }
        clustering_key_prefix&& get() {
            return std::move(prefix);
        }
        bool ends_with(const schema& s, const clustering_key_prefix& candidate) {
            return prefix.equal(s, candidate);
        }
        explicit operator sstring() const {
            return sprint("%s deletion %s", prefix, tomb);
        }
        friend std::ostream& operator<<(std::ostream& out, const open_tombstone& o) {
            return out << sstring(o);
        }
    };
    std::stack<open_tombstone> _open_tombstones;
    clustering_key_prefix _end_contiguous_delete;
public:
    range_tombstone_merger()
            : _end_contiguous_delete(std::vector<bytes>())
    { }
    void verify_no_open_tombstones() {
        // Verify that no range tombstone was left "open" (waiting to
        // be merged into a deletion of an entire row).
        // Should be called at the end of going through all range_tombstones.
        if (!_open_tombstones.empty()) {
            auto msg = sstring("RANGE DELETE not implemented. Tried to merge, but row finished before we could finish the merge. Starts found: (");
            while (!_open_tombstones.empty()) {
                msg += sstring(_open_tombstones.top());
                _open_tombstones.pop();
                if (!_open_tombstones.empty()) {
                    msg += sstring(" , ");
                }
            }
            msg += sstring(")");
            throw exceptions::unsupported_operation_exception(msg);
        }
    }

    std::experimental::optional<clustering_key_prefix> merge(const schema& s, const range_tombstone& rt) {
        if (!_open_tombstones.empty()) {
            // If the range tombstones are the result of Cassandra's splitting
            // overlapping tombstones into disjoint tombstones, they cannot
            // have a gap. If there is a gap while we're merging, it is
            // probably a bona-fide range delete, which we don't support.
            if (!_end_contiguous_delete.equal(s, rt.start)) {
                throw exceptions::unsupported_operation_exception(sprint(
                        "RANGE DELETE not implemented. Tried to merge but "
                        "found gap between %s and %s.",
                        _end_contiguous_delete, rt.start));
            }
        }
        if (_open_tombstones.empty() || rt.tomb.timestamp > _open_tombstones.top().tomb.timestamp) {
            _open_tombstones.push(open_tombstone(rt.start, rt.tomb));
        } else if (rt.tomb.timestamp < _open_tombstones.top().tomb.timestamp) {
            // If the new range has an *earlier* timestamp than the open tombstone
            // it is supposedely covering, then our representation as two overlapping
            // tombstones would not be identical to the two disjoint tombstones.
            throw exceptions::unsupported_operation_exception(sprint(
                        "RANGE DELETE not implemented. Tried to merge but "
                        "found range starting at %s which cannot close a "
                        "row because of decreasing timestamp %d.",
                        _open_tombstones.top(), rt.tomb.timestamp));
        } else if (rt.tomb.deletion_time != _open_tombstones.top().tomb.deletion_time) {
            // timestamps are equal, but deletion_times are not
            throw exceptions::unsupported_operation_exception(sprint(
                        "RANGE DELETE not implemented. Couldn't merge range "
                        "%s,%s into row %s. Both had same timestamp %s but "
                        "different deletion_time %s.",
                        rt.start, rt.end, _open_tombstones.top(),
                        rt.tomb.timestamp,
                        rt.tomb.deletion_time.time_since_epoch().count()));
        }
        std::experimental::optional<clustering_key_prefix> ret;
        if (_open_tombstones.top().ends_with(s, rt.end)) {
            ret = _open_tombstones.top().get();
            _open_tombstones.pop();
        }
        if (!_open_tombstones.empty()) {
            _end_contiguous_delete = rt.end;
        }
        return ret;
    }
};
