#
# Copyright (C) 2009-2019  Nexedi SA
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

import unittest
from ..mock import Mock, ReturnValues
from .. import NeoUnitTestBase
from neo.storage.app import Application
from neo.storage.handlers.client import ClientOperationHandler
from neo.lib.util import p64
from neo.lib.protocol import INVALID_TID, Packets

class StorageClientHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.app.tm = Mock({'__contains__': True})
        # handler
        self.operation = ClientOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getMasterUUID()
        pmn = self.app.nm.getMasterList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageClientHandlerTests, self)._tearDown(success)

    def _getConnection(self, uuid=None):
        return self.getFakeConnection(uuid=uuid, address=('127.0.0.1', 1000))

    def fakeDM(self, **kw):
        self.app.dm.close()
        self.app.dm = dm = Mock(kw)
        return dm

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = self._getConnection()
        self.fakeDM(getNumPartitions=1)
        self.operation.askTransactionInformation(conn, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_25_askTIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        self.fakeDM()
        conn = self._getConnection()
        self.checkProtocolErrorRaised(self.operation.askTIDs, conn, 1, 1, None)
        self.assertEqual(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEqual(len(app.dm.mockGetNamedCalls('getTIDList')), 0)

    def test_25_askTIDs2(self):
        # well case => answer
        conn = self._getConnection()
        self.app.pt = Mock({'getPartitions': 1})
        self.fakeDM(getTIDList=(INVALID_TID,))
        self.operation.askTIDs(conn, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(1, 1, [1, ])
        self.checkAnswerPacket(conn, Packets.AnswerTIDs)

    def test_26_askObjectHistory1(self):
        # invalid offsets => error
        dm = self.fakeDM()
        conn = self._getConnection()
        self.checkProtocolErrorRaised(self.operation.askObjectHistory, conn,
            1, 1, None)
        self.assertEqual(len(dm.mockGetNamedCalls('getObjectHistory')), 0)

    def test_askObjectUndoSerial(self):
        conn = self._getConnection(uuid=self.getClientUUID())
        tid = self.getNextTID()
        ltid = self.getNextTID()
        undone_tid = self.getNextTID()
        # Keep 2 entries here, so we check findUndoTID is called only once.
        oid_list = map(p64, (1, 2))
        self.app.tm = Mock({
            'getObjectFromTransaction': None,
        })
        self.fakeDM(findUndoTID=ReturnValues((None, None, False),))
        self.operation.askObjectUndoSerial(conn, tid, ltid, undone_tid, oid_list)
        self.checkErrorPacket(conn)

if __name__ == "__main__":
    unittest.main()
