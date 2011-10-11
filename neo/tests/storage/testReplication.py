#
# Copyright (C) 2010  Nexedi SA
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

import unittest
from mock import Mock
from struct import pack
from collections import deque

from neo.tests import NeoUnitTestBase
from neo.storage.database import buildDatabaseManager
from neo.storage.handlers.replication import ReplicationHandler
from neo.storage.handlers.replication import RANGE_LENGTH
from neo.storage.handlers.storage import StorageOperationHandler
from neo.storage.replicator import Replicator
from neo.lib.protocol import ZERO_OID, ZERO_TID

MAX_TRANSACTIONS = 10000
MAX_OBJECTS = 100000
MAX_TID = '\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE' # != INVALID_TID

class FakeConnection(object):

    def __init__(self):
        self._msg_id = 0
        self._queue = deque()

    def allocateId(self):
        self._msg_id += 1
        return self._msg_id

    def _addPacket(self, packet, *args, **kw):
        packet.setId(self.allocateId())
        self._queue.append(packet)
    ask = _addPacket
    answer = _addPacket
    notify = _addPacket

    def setPeerId(self, msg_id):
        pass

    def process(self, dhandler, dconn):
        if not self._queue:
            return False
        while self._queue:
            dhandler.dispatch(dconn, self._queue.popleft())
        return True

class ReplicationTests(NeoUnitTestBase):

    def checkReplicationProcess(self, reference, outdated):
        pt = Mock({'getPartitions': 1})
        # reference application
        rapp = Mock({})
        rapp.pt = pt
        rapp.dm = reference
        rapp.tm = Mock({'loadLocked': False})
        mconn = FakeConnection()
        rapp.master_conn = mconn
        # outdated application
        oapp = Mock({})
        oapp.dm = outdated
        oapp.pt = pt
        oapp.master_conn = mconn
        oapp.replicator = Replicator(oapp)
        oapp.replicator.getCurrentOffset = lambda: 0
        oapp.replicator.isCurrentConnection = lambda c: True
        oapp.replicator.getCurrentCriticalTID = lambda: MAX_TID
        # handlers and connections
        rhandler = StorageOperationHandler(rapp)
        rconn = FakeConnection()
        ohandler = ReplicationHandler(oapp)
        oconn = FakeConnection()
        # run replication
        ohandler.startReplication(oconn)
        process = True
        while process:
            process = oconn.process(rhandler, rconn)
            oapp.replicator.processDelayedTasks()
            process |= rconn.process(ohandler, oconn)
        # check transactions
        for tid in reference.getTIDList(0, MAX_TRANSACTIONS, 1, [0]):
            self.assertEqual(
                reference.getTransaction(tid),
                outdated.getTransaction(tid),
            )
        for tid in outdated.getTIDList(0, MAX_TRANSACTIONS, 1, [0]):
            self.assertEqual(
                outdated.getTransaction(tid),
                reference.getTransaction(tid),
            )
        # check transactions
        params = (ZERO_TID, '\xFF' * 8, MAX_TRANSACTIONS, 1, 0)
        self.assertEqual(
            reference.getReplicationTIDList(*params),
            outdated.getReplicationTIDList(*params),
        )
        # check objects
        params = (ZERO_OID, ZERO_TID, '\xFF' * 8, MAX_OBJECTS, 1, 0)
        self.assertEqual(
            reference.getObjectHistoryFrom(*params),
            outdated.getObjectHistoryFrom(*params),
        )

    def buildStorage(self, transactions, objects, name='BTree', config=None):
        def makeid(oid_or_tid):
            return pack('!Q', oid_or_tid)
        storage = buildDatabaseManager(name, config)
        storage.getNumPartitions = lambda: 1
        storage.setup(reset=True)
        storage._transactions = transactions
        storage._objects = objects
        # store transactions
        for tid in transactions:
            transaction = ([ZERO_OID], 'user', 'desc', '', False)
            storage.storeTransaction(makeid(tid), [], transaction, False)
        # store object history
        H = "0" * 20
        storage.storeData(H, '', 0)
        storage.unlockData((H,))
        for tid, oid_list in objects.iteritems():
            object_list = [(makeid(oid), H, None) for oid in oid_list]
            storage.storeTransaction(makeid(tid), object_list, None, False)
        return storage

    def testReplication0(self):
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=[1, 2, 3],
                objects={1: [1], 2: [1], 3: [1]},
            ),
            outdated=self.buildStorage(
                transactions=[],
                objects={},
            ),
        )

    def testReplication1(self):
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=[1, 2, 3],
                objects={1: [1], 2: [1], 3: [1]},
            ),
            outdated=self.buildStorage(
                transactions=[1],
                objects={1: [1]},
            ),
        )

    def testReplication2(self):
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=[1, 2, 3],
                objects={1: [1, 2, 3]},
            ),
            outdated=self.buildStorage(
                transactions=[1, 2, 3],
                objects={1: [1, 2, 3]},
            ),
        )

    def testChunkBeginning(self):
        ref_number = range(RANGE_LENGTH + 1)
        out_number = range(RANGE_LENGTH)
        obj_list = [1, 2, 3]
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=ref_number,
                objects=dict.fromkeys(ref_number, obj_list),
            ),
            outdated=self.buildStorage(
                transactions=out_number,
                objects=dict.fromkeys(out_number, obj_list),
            ),
        )

    def testChunkEnd(self):
        ref_number = range(RANGE_LENGTH)
        out_number = range(RANGE_LENGTH - 1)
        obj_list = [1, 2, 3]
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=ref_number,
                objects=dict.fromkeys(ref_number, obj_list)
            ),
            outdated=self.buildStorage(
                transactions=out_number,
                objects=dict.fromkeys(out_number, obj_list)
            ),
        )

    def testChunkMiddle(self):
        obj_list = [1, 2, 3]
        ref_number = range(RANGE_LENGTH)
        out_number = range(4000)
        out_number.remove(3000)
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=ref_number,
                objects=dict.fromkeys(ref_number, obj_list)
            ),
            outdated=self.buildStorage(
                transactions=out_number,
                objects=dict.fromkeys(out_number, obj_list)
            ),
        )

    def testFullChunkPart(self):
        obj_list = [1, 2, 3]
        ref_number = range(1001)
        out_number = {}
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=ref_number,
                objects=dict.fromkeys(ref_number, obj_list)
            ),
            outdated=self.buildStorage(
                transactions=out_number,
                objects=dict.fromkeys(out_number, obj_list)
            ),
        )

    def testSameData(self):
        obj_list = [1, 2, 3]
        number = range(RANGE_LENGTH * 2)
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=number,
                objects=dict.fromkeys(number, obj_list)
            ),
            outdated=self.buildStorage(
                transactions=number,
                objects=dict.fromkeys(number, obj_list)
            ),
        )

    def testTooManyData(self):
        obj_list = [0, 1]
        ref_number = range(RANGE_LENGTH)
        out_number = range(RANGE_LENGTH + 2)
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=ref_number,
                objects=dict.fromkeys(ref_number, obj_list)
            ),
            outdated=self.buildStorage(
                transactions=out_number,
                objects=dict.fromkeys(out_number, obj_list)
            ),
        )

    def testMissingObject(self):
        self.checkReplicationProcess(
            reference=self.buildStorage(
                transactions=[1, 2],
                objects=dict.fromkeys([1, 2], [1, 2]),
            ),
            outdated=self.buildStorage(
                transactions=[1, 2],
                objects=dict.fromkeys([1], [1]),
            ),
        )

if __name__ == "__main__":
    unittest.main()
