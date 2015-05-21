#
# Copyright (C) 2006-2015  Nexedi SA
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
from neo.lib.protocol import ZERO_TID, MAX_TID
from neo.lib.util import u64, add64
from .exception import NEOStorageCreationUndoneError, NEOStorageNotFoundError

CHUNK_LENGTH = 100

class Record(BaseStorage.DataRecord):
    """ BaseStorage Transaction record yielded by the Transaction object """

    def __str__(self):
        oid = u64(self.oid)
        tid = u64(self.tid)
        args = (oid, tid, len(self.data), self.data_txn)
        return 'Record %s:%s: %s (%s)' % args


class Transaction(BaseStorage.TransactionRecord):
    """ Transaction object yielded by the NEO iterator """

    def __init__(self, app, txn):
        super(Transaction, self).__init__(txn['id'], ' ',
            txn['user_name'], txn['description'], txn['ext'])
        self.app = app
        self.oid_list = txn['oids']

    def __iter__(self):
        """ Iterate over the transaction records """
        load = self.app._loadFromStorage
        for oid in self.oid_list:
            try:
                data, _, _, data_tid = load(oid, self.tid, None)
            except NEOStorageCreationUndoneError:
                data = data_tid = None
            except NEOStorageNotFoundError:
                # Transactions are not updated after a pack, so their object
                # will not be found in the database. Skip them.
                continue
            yield Record(oid, self.tid, data, data_tid)

    def __str__(self):
        return 'Transaction #%s: %s %s' \
            % (u64(self.tid), self.user, self.status)


def iterator(app, start=None, stop=None):
    """NEO transaction iterator"""
    if start is None:
        start = ZERO_TID
    stop = min(stop or MAX_TID, app.lastTransaction())
    while 1:
        max_tid, chunk = app.transactionLog(start, stop, CHUNK_LENGTH)
        if not chunk:
            break # nothing more
        for txn in chunk:
            yield Transaction(app, txn)
        start = add64(max_tid, 1)
