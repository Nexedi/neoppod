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

from .. import MockObject, NeoUnitTestBase
from neo.storage.app import Application
from neo.storage.handlers.client import ClientOperationHandler
from neo.lib.exception import ProtocolError
from neo.lib.protocol import INVALID_TID, Packets
from neo.lib.util import p64

class StorageClientHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.operation = ClientOperationHandler(self.app)

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageClientHandlerTests, self)._tearDown(success)

    def fakeDM(self, **kw):
        self.app.dm.close()
        self.app.dm = dm = MockObject(**kw)
        return dm

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = self.getFakeConnection()
        self.fakeDM(getTransaction=None)
        self.operation.askTransactionInformation(conn, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_askTIDs(self):
        conn = self.getFakeConnection()
        self.app.pt = MockObject(getPartitions=1)
        dm = self.fakeDM(getTIDList=(INVALID_TID,))
        self.operation.askTIDs(conn, 1, 2, 1)
        dm.getTIDList.assert_called_once_with(1, 1, [1, ])
        self.checkAnswerPacket(conn, Packets.AnswerTIDs)

    def test_askObjectUndoSerial(self):
        conn = self.getFakeConnection(self.getClientUUID())
        tid = self.getNextTID()
        ltid = self.getNextTID()
        undone_tid = self.getNextTID()
        # Keep 2 entries here, so we check findUndoTID is called only once.
        oid_list = map(p64, (1, 2))
        self.app.tm = MockObject(getObjectFromTransaction=None)
        dm = self.fakeDM()
        dm.findUndoTID.side_effect = (None, None, False),
        self.operation.askObjectUndoSerial(conn, tid, ltid, undone_tid, oid_list)
        self.checkErrorPacket(conn)
