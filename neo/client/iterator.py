#
# Copyright (C) 2006-2011  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from ZODB import BaseStorage
from neo.lib.util import u64, add64
from .exception import NEOStorageCreationUndoneError, NEOStorageNotFoundError

CHUNK_LENGTH = 100

class Record(BaseStorage.DataRecord):
    """ BaseStorage Transaction record yielded by the Transaction object """

    def __init__(self, oid, tid, data, prev):
        BaseStorage.DataRecord.__init__(self, oid, tid, data, prev)

    def __str__(self):
        oid = u64(self.oid)
        tid = u64(self.tid)
        args = (oid, tid, len(self.data), self.data_txn)
        return 'Record %s:%s: %s (%s)' % args


class Transaction(BaseStorage.TransactionRecord):
    """ Transaction object yielded by the NEO iterator """

    def __init__(self, app, tid, status, user, desc, ext, oid_list,
            prev_serial_dict):
        BaseStorage.TransactionRecord.__init__( self, tid, status, user, desc,
            ext)
        self.app = app
        self.oid_list = oid_list
        self.oid_index = 0
        self.history = []
        self.prev_serial_dict = prev_serial_dict

    def __iter__(self):
        return self

    def next(self):
        """ Iterate over the transaction records """
        app = self.app
        oid_list = self.oid_list
        oid_index = self.oid_index
        oid_len = len(oid_list)
        # load an object
        while oid_index < oid_len:
            oid = oid_list[oid_index]
            try:
                data, _, next_tid = app.load(oid, self.tid)
            except NEOStorageCreationUndoneError:
                data = next_tid = None
            except NEOStorageNotFoundError:
                # Transactions are not updated after a pack, so their object
                # will not be found in the database. Skip them.
                oid_list.pop(oid_index)
                oid_len -= 1
                continue
            oid_index += 1
            break
        else:
            # no more records for this transaction
            self.oid_index = 0
            raise StopIteration
        self.oid_index = oid_index
        record = Record(oid, self.tid, data,
            self.prev_serial_dict.get(oid))
        if next_tid is None:
            self.prev_serial_dict.pop(oid, None)
        else:
            self.prev_serial_dict[oid] = self.tid
        return record

    def __str__(self):
        tid = u64(self.tid)
        args = (tid, self.user, self.status)
        return 'Transaction #%s: %s %s' % args


class Iterator(object):
    """ An iterator for the NEO storage """

    def __init__(self, app, start, stop):
        self.app = app
        self._txn_list = []
        assert None not in (start, stop)
        self._start = start
        self._stop = stop
        # index of current iteration
        self._index = 0
        self._closed = False
        # OID -> previous TID mapping
        # TODO: prune old entries while walking ?
        self._prev_serial_dict = {}

    def __iter__(self):
        return self

    def __getitem__(self, index):
        """ Simple index-based iterator """
        if index != self._index:
            raise IndexError, index
        return self.next()

    def next(self):
        """ Return an iterator for the next transaction"""
        if self._closed:
            raise IOError, 'iterator closed'
        if not self._txn_list:
            (max_tid, chunk) = self.app.transactionLog(self._start, self._stop,
                CHUNK_LENGTH)
            if not chunk:
                # nothing more
                raise StopIteration
            self._start = add64(max_tid, 1)
            self._txn_list = chunk
        txn = self._txn_list.pop(0)
        self._index += 1
        tid = txn['id']
        user = txn['user_name']
        desc = txn['description']
        oid_list = txn['oids']
        extension = txn['ext']
        txn = Transaction(self.app, tid, ' ', user, desc, extension, oid_list,
            self._prev_serial_dict)
        return txn

    def __str__(self):
        return 'NEO transactions iterator'

    def close(self):
        self._closed = True
