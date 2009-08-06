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

from time import sleep, time
import unittest
from neo.tests.functional import NEOCluster
from neo.neoctl.neoctl import NotReadyException
from neo import protocol
from neo.util import dump

MASTER_NODE_COUNT = 3

neo = NEOCluster([], port_base=20000, master_node_count=MASTER_NODE_COUNT)

class MasterTests(unittest.TestCase):

    def setUp(self):
        neo.stop()
        neo.start()
        self.storage = neo.getZODBStorage()
        self.neoctl = neo.getNEOCTL()

    def tearDown(self):
        neo.stop()

    def testStoppingSecondaryMaster(self):
        # Wait for masters to stabilize
        neo.expectAllMasters(MASTER_NODE_COUNT)

        # Kill
        killed_uuid_list = neo.killSecondaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Check node state has changed.
        neo.expectMasterState(uuid, None)

    def testStoppingPrimaryMasterWithTwoSecondaries(self):
        # Wait for masters to stabilize
        neo.expectAllMasters(MASTER_NODE_COUNT)

        # Kill
        killed_uuid_list = neo.killPrimaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Check the state of the primary we just killed
        neo.expectMasterState(uuid, (None, protocol.UNKNOWN_STATE))
        self.assertEqual(neo.getPrimaryMaster(), None)
        # Check that a primary master arised.
        neo.expectPrimaryMaster(timeout=10)
        # Check that the uuid really changed.
        new_uuid = neo.getPrimaryMaster()
        self.assertNotEqual(new_uuid, uuid)

    def testStoppingPrimaryMasterWithOneSecondary(self):
        neo.expectAllMasters(MASTER_NODE_COUNT, state=protocol.RUNNING_STATE)

        # Kill one secondary master.
        killed_uuid_list = neo.killSecondaryMaster()
        # Test sanity checks.
        self.assertEqual(len(killed_uuid_list), 1)
        neo.expectMasterState(killed_uuid_list[0], None)
        self.assertEqual(len(neo.getMasterNodeList()), 2)

        killed_uuid_list = neo.killPrimaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Check the state of the primary we just killed
        neo.expectMasterState(uuid, (None, protocol.UNKNOWN_STATE))
        self.assertEqual(neo.getPrimaryMaster(), None)
        # Check that a primary master arised.
        neo.expectPrimaryMaster(timeout=10)
        # Check that the uuid really changed.
        new_uuid = neo.getPrimaryMaster()
        self.assertNotEqual(new_uuid, uuid)

    def testMasterSequentialStart(self):
        neo.expectAllMasters(MASTER_NODE_COUNT, state=protocol.RUNNING_STATE)
        master_list = neo.getMasterProcessList()

        # Stop the cluster (so we can start processes manually)
        neo.killMasters()

        # Start the first master.
        first_master = master_list[0]
        first_master.start()
        first_master_uuid = first_master.getUUID()
        # Check that the master node we started elected itself.
        neo.expectPrimaryMaster(first_master_uuid, timeout=60)
        # Check that no other node is known as running.
        self.assertEqual(len(neo.getMasterNodeList(
            state=protocol.RUNNING_STATE)), 1)

        # Start a second master.
        second_master = master_list[1]
        # Check that the second master is known as being down.
        self.assertEqual(neo.getMasterNodeState(second_master.getUUID()), None)
        second_master.start()
        # Check that the second master is running under his known UUID.
        neo.expectMasterState(second_master.getUUID(), protocol.RUNNING_STATE)
        # Check that the primary master didn't change.
        self.assertEqual(neo.getPrimaryMaster(), first_master_uuid)

        # Start a third master.
        third_master = master_list[2]
        # Check that the third master is known as being down.
        self.assertEqual(neo.getMasterNodeState(third_master.getUUID()), None)
        third_master.start()
        # Check that the third master is running under his known UUID.
        neo.expectMasterState(third_master.getUUID(), protocol.RUNNING_STATE)
        # Check that the primary master didn't change.
        self.assertEqual(neo.getPrimaryMaster(), first_master_uuid)

def test_suite():
    return unittest.makeSuite(MasterTests)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")

