#
# Copyright (C) 2009  Nexedi SA
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
from collections import deque
from neo.tests import NeoTestBase
from neo.storage.app import Application
from neo.storage.handlers.storage import StorageOperationHandler
from neo.protocol import Packets, Packet, INVALID_PARTITION
from neo.protocol import INVALID_TID, INVALID_OID, INVALID_SERIAL

class StorageStorageHandlerTests(NeoTestBase):

    def checkHandleUnexpectedPacket(self, _call, _msg_type, _listening=True, **kwargs):
        conn = Mock({
            "getAddress" : ("127.0.0.1", self.master_port),
            "isServer": _listening,
        })
        packet = Packet(msg_type=_msg_type)
        # hook
        self.operation.peerBroken = lambda c: c.peerBrokendCalled()
        self.checkUnexpectedPacketRaised(_call, conn=conn, packet=packet, **kwargs)

    def setUp(self):
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.app.transaction_dict = {}
        self.app.store_lock_dict = {}
        self.app.load_lock_dict = {}
        self.app.event_queue = deque()
        for address in self.app.master_node_list:
            self.app.nm.createMaster(address=address)
        # handler
        self.operation = StorageOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getNewUUID()
        pmn = self.app.nm.getMasterList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn
        self.master_port = 10010

    def tearDown(self):
        NeoTestBase.tearDown(self)

    def test_18_askTransactionInformation1(self):
        # transaction does not exists
        conn = Mock({ })
        packet = Packets.AskTransactionInformation()
        packet.setId(0)
        self.operation.askTransactionInformation(conn, packet, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_18_askTransactionInformation2(self):
        # answer
        conn = Mock({ })
        packet = Packets.AskTransactionInformation()
        packet.setId(0)
        dm = Mock({ "getTransaction": (INVALID_TID, 'user', 'desc', '', ), })
        self.app.dm = dm
        self.operation.askTransactionInformation(conn, packet, INVALID_TID)
        self.checkAnswerTransactionInformation(conn)

    def test_24_askObject1(self):
        # delayed response
        conn = Mock({})
        self.app.dm = Mock()
        packet = Packets.AskObject()
        self.app.load_lock_dict[INVALID_OID] = object()
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, packet,
            oid=INVALID_OID,
            serial=INVALID_SERIAL,
            tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 1)
        self.checkNoPacketSent(conn)
        self.assertEquals(len(self.app.dm.mockGetNamedCalls('getObject')), 0)

    def test_24_askObject2(self):
        # invalid serial / tid / packet not found
        self.app.dm = Mock({'getObject': None})
        conn = Mock({})
        packet = Packets.AskObject()
        packet.setId(0)
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, packet,
            oid=INVALID_OID,
            serial=INVALID_SERIAL,
            tid=INVALID_TID)
        calls = self.app.dm.mockGetNamedCalls('getObject')
        self.assertEquals(len(self.app.event_queue), 0)
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(INVALID_OID, INVALID_TID, INVALID_TID)
        self.checkErrorPacket(conn)

    def test_24_askObject3(self):
        # object found => answer
        self.app.dm = Mock({'getObject': ('', '', 0, 0, '', )})
        conn = Mock({})
        packet = Packets.AskObject()
        packet.setId(0)
        self.assertEquals(len(self.app.event_queue), 0)
        self.operation.askObject(conn, packet,
            oid=INVALID_OID,
            serial=INVALID_SERIAL,
            tid=INVALID_TID)
        self.assertEquals(len(self.app.event_queue), 0)
        self.checkAnswerObject(conn)

    def test_25_askTIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = Mock({})
        packet = Packets.AskTIDs()
        self.checkProtocolErrorRaised(self.operation.askTIDs, conn, packet, 1, 1, None)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getTIDList')), 0)

    def test_25_askTIDs2(self):
        # well case => answer
        conn = Mock({})
        packet = Packets.AskTIDs()
        packet.setId(0)
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.app.pt = Mock({'getPartitions': 1})
        self.operation.askTIDs(conn, packet, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [1, ])
        self.checkAnswerTids(conn)

    def test_25_askTIDs3(self):
        # invalid partition => answer usable partitions
        conn = Mock({})
        packet = Packets.AskTIDs()
        packet.setId(0)
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getTIDList': (INVALID_TID, )})
        self.app.pt = Mock({'getCellList': (cell, ), 'getPartitions': 1})
        self.operation.askTIDs(conn, packet, 1, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getCellList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getTIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [0, ])
        self.checkAnswerTids(conn)

    def test_26_askObjectHistory1(self):
        # invalid offsets => error
        app = self.app
        app.dm = Mock()
        conn = Mock({})
        packet = Packets.AskObjectHistory()
        packet.setId(0)
        self.checkProtocolErrorRaised(self.operation.askObjectHistory, conn, packet, 1, 1, None)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getObjectHistory')), 0)

    def test_26_askObjectHistory2(self):
        # first case: empty history
        packet = Packets.AskObjectHistory()
        packet.setId(0)
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': None})
        self.operation.askObjectHistory(conn, packet, INVALID_OID, 1, 2)
        self.checkAnswerObjectHistory(conn)
        # second case: not empty history
        conn = Mock({})
        self.app.dm = Mock({'getObjectHistory': [('', 0, ), ]})
        self.operation.askObjectHistory(conn, packet, INVALID_OID, 1, 2)
        self.checkAnswerObjectHistory(conn)

    def test_25_askOIDs1(self):
        # invalid offsets => error
        app = self.app
        app.pt = Mock()
        app.dm = Mock()
        conn = Mock({})
        packet = Packets.AskOIDs()
        packet.setId(0)
        self.checkProtocolErrorRaised(self.operation.askOIDs, conn, packet, 1, 1, None)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
        self.assertEquals(len(app.dm.mockGetNamedCalls('getOIDList')), 0)

    def test_25_askOIDs2(self):
        # well case > answer OIDs
        conn = Mock({})
        packet = Packets.AskOIDs()
        packet.setId(0)
        self.app.pt = Mock({'getPartitions': 1})
        self.app.dm = Mock({'getOIDList': (INVALID_OID, )})
        self.operation.askOIDs(conn, packet, 1, 2, 1)
        calls = self.app.dm.mockGetNamedCalls('getOIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [1, ])
        self.checkAnswerOids(conn)

    def test_25_askOIDs3(self):
        # invalid partition => answer usable partitions
        conn = Mock({})
        packet = Packets.AskOIDs()
        packet.setId(0)
        cell = Mock({'getUUID':self.app.uuid})
        self.app.dm = Mock({'getOIDList': (INVALID_OID, )})
        self.app.pt = Mock({'getCellList': (cell, ), 'getPartitions': 1})
        self.operation.askOIDs(conn, packet, 1, 2, INVALID_PARTITION)
        self.assertEquals(len(self.app.pt.mockGetNamedCalls('getCellList')), 1)
        calls = self.app.dm.mockGetNamedCalls('getOIDList')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs(1, 1, 1, [0, ])
        self.checkAnswerOids(conn)


if __name__ == "__main__":
    unittest.main()
