#
# Copyright (C) 2010-2011  Nexedi SA
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
from neo.client.mq import MQIndex

class RevisionIndex(MQIndex):
    """
    This cache index allows accessing a specifig revision of a cached object.
    It requires cache key to be a 2-tuple, composed of oid and revision.

    Note: it is expected that rather few revisions are held in cache, with few
    lookups for old revisions, so they are held in a simple sorted list
    Note2: all methods here must be called with cache lock acquired.
    """
    def __init__(self):
        # key: oid
        # value: tid list, from highest to lowest
        self._oid_dict = {}
        # key: oid
        # value: tid list, from lowest to highest
        self._invalidated = {}

    def clear(self):
        self._oid_dict.clear()
        self._invalidated.clear()

    def remove(self, key):
        oid_dict = self._oid_dict
        oid, tid = key
        tid_list = oid_dict[oid]
        tid_list.remove(tid)
        if not tid_list:
            # No more serial known for this object, drop entirely
            del oid_dict[oid]
            self._invalidated.pop(oid, None)

    def add(self, key):
        oid_dict = self._oid_dict
        oid, tid = key
        try:
            serial_list = oid_dict[oid]
        except KeyError:
            serial_list = oid_dict[oid] = []
        else:
            assert tid not in serial_list
        if not(serial_list) or tid > serial_list[0]:
            serial_list.insert(0, tid)
        else:
            serial_list.insert(0, tid)
            serial_list.sort(reverse=True)
        invalidated = self._invalidated
        try:
            tid_list = invalidated[oid]
        except KeyError:
            pass
        else:
            try:
                tid_list.remove(tid)
            except ValueError:
                pass
            else:
                if not tid_list:
                    del invalidated[oid]

    def invalidate(self, oid_list, tid):
        """
        Mark object invalidated by given transaction.
        Must be called with increasing TID values (which is standard for
        ZODB).
        """
        invalidated = self._invalidated
        oid_dict = self._oid_dict
        for oid in (x for x in oid_list if x in oid_dict):
            try:
                tid_list = invalidated[oid]
            except KeyError:
                tid_list = invalidated[oid] = []
            assert not tid_list or tid > tid_list[-1], (dump(oid), dump(tid),
                dump(tid_list[-1]))
            tid_list.append(tid)

    def getSerialBefore(self, oid, tid):
        """
        Get the first tid in cache which value is lower that given tid.
        """
        # WARNING: return-intensive to save on indentation
        oid_list = self._oid_dict.get(oid)
        if oid_list is None:
            # Unknown oid
            return None
        for result in oid_list:
            if result < tid:
                # Candidate found
                break
        else:
            # No candidate in cache.
            return None
        # Check if there is a chance that an intermediate revision would
        # exist, while missing from cache.
        try:
            inv_tid_list = self._invalidated[oid]
        except KeyError:
            return result
        # Remember: inv_tid_list is sorted in ascending order.
        for inv_tid in inv_tid_list:
            if tid < inv_tid:
                # We don't care about invalidations past requested TID.
                break
            elif result < inv_tid < tid:
                # An invalidation was received between candidate revision,
                # and before requested TID: there is a matching revision we
                # don't know of, so we cannot answer.
                return None
        return result

    def getLatestSerial(self, oid):
        """
        Get the latest tid for given object.
        """
        result = self._oid_dict.get(oid)
        if result is not None:
            result = result[0]
            try:
                tid_list = self._invalidated[oid]
            except KeyError:
                pass
            else:
                if result < tid_list[-1]:
                    # An invalidation happened from a transaction later than
                    # our most recent view of this object, so we cannot answer.
                    result = None
        return result

    def getSerialList(self, oid):
        """
        Get the list of all serials cache knows about for given object.
        """
        return self._oid_dict.get(oid, [])[:]

