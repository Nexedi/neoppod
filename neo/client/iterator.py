#
# Copyright (C) 2006-2010  Nexedi SA
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

from ZODB import BaseStorage
from zope.interface import implements
import ZODB.interfaces
from neo import util
from neo.client.exception import NEOStorageCreationUndoneError
from neo.client.exception import NEOStorageNotFoundError

class Record(BaseStorage.DataRecord):
    """ TBaseStorageransaction record yielded by the Transaction object """

    implements(
        ZODB.interfaces.IStorageRecordInformation,
    )

    def __init__(self, oid, tid, version, data, prev):
        self.oid = oid
        self.tid = tid
        self.version = version
        self.data = data
        self.data_txn = prev

    def __str__(self):
        oid = util.u64(self.oid)
        tid = util.u64(self.tid)
        args = (oid, tid, len(self.data), self.data_txn)
        return 'Record %s:%s: %s (%s)' % args


class Transaction(BaseStorage.TransactionRecord):
    """ Transaction object yielded by the NEO iterator """

    implements(
        # TODO: add support for "extension" property so we implement entirely
        # this interface.
        # ZODB.interfaces.IStorageTransactionInformation,
    )

    def __init__(self, app, tid, status, user, desc, ext, oid_list,
            prev_serial_dict):
        self.app = app
        self.tid = tid
        self.status = status
        self.user = user
        self.description = desc
        self._extension = ext
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
                data, _, next_tid = app._load(oid, serial=self.tid)
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
        record = Record(oid, self.tid, '', data,
            self.prev_serial_dict.get(oid))
        if next_tid is None:
            self.prev_serial_dict.pop(oid, None)
        else:
            self.prev_serial_dict[oid] = self.tid
        return record

    def __str__(self):
        tid = util.u64(self.tid)
        args = (tid, self.user, self.status)
        return 'Transaction #%s: %s %s' % args


class Iterator(object):
    """ An iterator for the NEO storage """

    def __init__(self, app, start, stop):
        self.app = app
        self.txn_list = []
        self._stop = stop
        # next index to load from storage nodes
        self._next = 0
        # index of current iteration
        self._index = 0
        self._closed = False
        # OID -> previous TID mapping
        # TODO: prune old entries while walking ?
        self._prev_serial_dict = {}
        if start is not None:
            self.txn_list = self._skip(start)

    def __iter__(self):
        return self

    def __getitem__(self, index):
        """ Simple index-based iterator """
        if index != self._index:
            raise IndexError, index
        return self.next()

    def _read(self):
        """ Request more transactions """
        chunk = self.app.transactionLog(self._next, self._next + 100)
        if not chunk:
            # nothing more
            raise StopIteration
        self._next += len(chunk)
        return chunk

    def _skip(self, start):
        """ Skip transactions until 'start' is reached """
        chunk = self._read()
        while chunk[0]['id'] < start:
            chunk = self._read()
        if chunk[-1]['id'] < start:
            for index, txn in enumerate(reversed(chunk)):
                if txn['id'] >= start:
                    break
            # keep only greater transactions
            chunk = chunk[:-index]
        return chunk

    def next(self):
        """ Return an iterator for the next transaction"""
        if self._closed:
            raise IOError, 'iterator closed'
        if not self.txn_list:
            self.txn_list = self._read()
        txn = self.txn_list.pop()
        self._index += 1
        tid = txn['id']
        stop = self._stop
        if stop is not None and stop < tid:
            # stop reached
            raise StopIteration
        user = txn['user_name']
        desc = txn['description']
        oid_list = txn['oids']
        extension = {} # as expected by the ZODB
        txn = Transaction(self.app, tid, ' ', user, desc, extension, oid_list,
            self._prev_serial_dict)
        return txn

    def __str__(self):
        return 'NEO transactions iteratpr'

    def close(self):
        self._closed = True
