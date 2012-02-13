#
# Copyright (C) 2011  Nexedi SA
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

from thread import get_ident
from neo.lib.locking import Queue

class ContainerBase(object):
    def __init__(self):
        self._context_dict = {}

    def _getID(self, *args, **kw):
        raise NotImplementedError

    def _new(self, *args, **kw):
        raise NotImplementedError

    def delete(self, *args, **kw):
        del self._context_dict[self._getID(*args, **kw)]

    def get(self, *args, **kw):
        return self._context_dict.get(self._getID(*args, **kw))

    def new(self, *args, **kw):
        result = self._context_dict[self._getID(*args, **kw)] = self._new(
            *args, **kw)
        return result

class ThreadContainer(ContainerBase):
    def _getID(self):
        return get_ident()

    def _new(self):
        return {
            'queue': Queue(0),
            'answer': None,
        }

    def get(self):
        """
        Implicitely create a thread context if it doesn't exist.
        """
        my_id = self._getID()
        try:
            result = self._context_dict[my_id]
        except KeyError:
            result = self._context_dict[my_id] = self._new()
        return result

class TransactionContainer(ContainerBase):
    def _getID(self, txn):
        return id(txn)

    def _new(self, txn):
        return {
            'queue': Queue(0),
            'txn': txn,
            'ttid': None,
            'data_dict': {},
            'data_size': 0,
            'cache_dict': {},
            'cache_size': 0,
            'object_base_serial_dict': {},
            'object_serial_dict': {},
            'object_stored_counter_dict': {},
            'conflict_serial_dict': {},
            'resolved_conflict_serial_dict': {},
            'txn_voted': False,
            'involved_nodes': set(),
        }

