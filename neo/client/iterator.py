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
from neo import util


class Record(BaseStorage.DataRecord):
    """ TBaseStorageransaction record yielded by the Transaction object """

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

    def __init__(self, app, tid, status, user, desc, ext, oid_list,
            prev_serial_dict):
        self.app = app
        self.tid = tid
        self.status = status
        self.user = user
        self.description = desc
        self._extension = ext
        self.oid_list = oid_list
        self.history = []
        self.prev_serial_dict = prev_serial_dict

    def __iter__(self):
        return self

    def next(self):
        """ Iterate over the transaction records """
        app = self.app
        if not self.oid_list:
            # no more records for this transaction
            raise StopIteration
        oid = self.oid_list.pop()
        # load an object
        data, _, next_tid = app._load(oid, serial=self.tid)
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
        if start is not None or stop is not None:
            raise NotImplementedError('partial scan not implemented yet')
        self.app = app
        self.txn_list = []
        # next index to load from storage nodes
        self._next = 0
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
        app = self.app
        if not self.txn_list:
            # ask some transactions
            self.txn_list = app.transactionLog(self._next, self._next + 100)
            if not self.txn_list:
                # scan finished
                raise StopIteration
            self._next += len(self.txn_list)
        txn = self.txn_list.pop()
        self._index += 1
        tid = txn['id']
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
