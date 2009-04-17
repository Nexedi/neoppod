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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import unittest
from mock import Mock, ReturnValues
from neo.protocol import INVALID_UUID

from neo.client.app import Application
from neo.protocol import Packet
import os

def connectToPrimaryMasterNode(self):
    # TODO: remove monkeypatching and properly simulate master node connection
    pass
Application.connectToPrimaryMasterNode_org = Application.connectToPrimaryMasterNode
Application.connectToPrimaryMasterNode = connectToPrimaryMasterNode

def _waitMessage(self, conn=None, msg_id=None):
    if conn is None:
        raise NotImplementedError
    self.answer_handler.dispatch(conn, conn.fakeReceived())
Application._waitMessage_org = Application._waitMessage
Application._waitMessage = _waitMessage

class TestSocketConnector(object):
    """
      Test socket connector.
      Exports both an API compatible with neo.connector.SocketConnector
      and a test-only API which allows sending packets to created connectors.
    """
    def __init__(self):
        raise NotImplementedError

    def makeClientConnection(self, addr):
        raise NotImplementedError

    def makeListeningConnection(self, addr):
        raise NotImplementedError

    def getError(self):
        raise NotImplementedError

    def getDescriptor(self):
        raise NotImplementedError

    def getNewConnection(self):
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError
    
    def receive(self):
        raise NotImplementedError

    def send(self, msg):
        raise NotImplementedError

class ConnectionPoolTest(unittest.TestCase):

    def getUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def getApp(self, master_nodes='127.0.0.1:10010', name='test',
               connector='SocketConnector', **kw):
        # TODO: properly simulate master node connection
        return Application(master_nodes, name, connector, **kw)

    def test_getQueue(self):
        app = self.getApp()
        # Test sanity check
        self.assertTrue(getattr(app, 'local_var', None) is not None)
        # Test that queue is created if it does not exist in local_var
        self.assertTrue(getattr(app.local_var, 'queue', None) is None)
        queue = app.getQueue()
        # Test sanity check
        self.assertTrue(getattr(app.local_var, 'queue', None) is queue)

    def test_registerDB(self):
        app = self.getApp()
        dummy_db = []
        app.registerDB(dummy_db, None)
        self.assertTrue(app.getDB() is dummy_db)

    def test_new_oid(self):
        app = self.getApp()
        test_msg_id = 50
        test_oid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        response_packet = Packet()
        response_packet.answerNewOIDs(test_msg_id, test_oid_list[:])
        app.master_conn = Mock({'getNextId': test_msg_id, 'addPacket': None,
                                'expectMessage': None, 'lock': None,
                                'unlock': None,
                                # Test-specific method
                                'fakeReceived': response_packet})
        new_oid = app.new_oid()
        self.assertTrue(new_oid in test_oid_list)
        self.assertEqual(len(app.new_oid_list), 1)
        self.assertTrue(app.new_oid_list[0] in test_oid_list)
        self.assertNotEqual(app.new_oid_list[0], new_oid)

    def test_getSerial(self):
        raise NotImplementedError

    def test_load(self):
        raise NotImplementedError

    def test_loadSerial(self):
        raise NotImplementedError

    def test_loadBefore(self):
        raise NotImplementedError

    def test_tpc_begin(self):
        raise NotImplementedError

    def test_store(self):
        raise NotImplementedError

    def test_tpc_vote(self):
        raise NotImplementedError

    def test_tpc_abort(self):
        raise NotImplementedError

    def test_tpc_finish(self):
        raise NotImplementedError

    def test_undo(self):
        raise NotImplementedError

    def test_undoLog(self):
        raise NotImplementedError

    def test_history(self):
        raise NotImplementedError

    def test_sync(self):
        raise NotImplementedError

    def test_connectToPrimaryMasterNode(self):
        raise NotImplementedError
        app = getApp()
        app.connectToPrimaryMasterNode_org()

if __name__ == '__main__':
    unittest.main()

