#
# Copyright (C) 2009-2015  Nexedi SA
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
from . import NEOCluster, NEOFunctionalTest
from neo.lib.protocol import NodeStates

MASTER_NODE_COUNT = 3


class MasterTests(NEOFunctionalTest):

    def setUp(self):
        NEOFunctionalTest.setUp(self)
        self.neo = NEOCluster([], master_count=MASTER_NODE_COUNT,
                temp_dir=self.getTempDirectory())
        self.neo.stop()
        self.neo.run()

    def _tearDown(self, success):
        self.neo.stop()
        NEOFunctionalTest._tearDown(self, success)

    def testStoppingSecondaryMaster(self):
        # Wait for masters to stabilize
        self.neo.expectAllMasters(MASTER_NODE_COUNT, NodeStates.RUNNING)

        # Kill
        neoctl = self.neo.neoctl
        primary_uuid = neoctl.getPrimary()
        for master in self.neo.getMasterProcessList():
            uuid = master.getUUID()
            if uuid != primary_uuid:
                break
        neoctl.killNode(uuid)
        self.neo.expectDead(master)
        self.assertRaises(RuntimeError, neoctl.killNode, primary_uuid)

    def testStoppingPrimaryWithTwoSecondaries(self):
        # Wait for masters to stabilize
        self.neo.expectAllMasters(MASTER_NODE_COUNT)

        # Kill
        killed_uuid_list = self.neo.killPrimary()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Check the state of the primary we just killed
        self.neo.expectMasterState(uuid, (None, NodeStates.UNKNOWN))
        # BUG: The following check expects neoctl to reconnect before
        #      the election finishes.
        self.assertEqual(self.neo.getPrimary(), None)
        # Check that a primary master arised.
        self.neo.expectPrimary(timeout=10)
        # Check that the uuid really changed.
        new_uuid = self.neo.getPrimary()
        self.assertNotEqual(new_uuid, uuid)

    def testStoppingPrimaryWithOneSecondary(self):
        self.neo.expectAllMasters(MASTER_NODE_COUNT,
                state=NodeStates.RUNNING)

        # Kill one secondary master.
        killed_uuid_list = self.neo.killSecondaryMaster()
        # Test sanity checks.
        self.assertEqual(len(killed_uuid_list), 1)
        self.neo.expectMasterState(killed_uuid_list[0], None)
        self.assertEqual(len(self.neo.getMasterList()), 2)

        uuid, = self.neo.killPrimary()
        # Check the state of the primary we just killed
        self.neo.expectMasterState(uuid, (None, NodeStates.UNKNOWN))
        # Check that a primary master arised.
        self.neo.expectPrimary(timeout=10)
        # Check that the uuid really changed.
        self.assertNotEqual(self.neo.getPrimary(), uuid)

    def testMasterSequentialStart(self):
        self.neo.expectAllMasters(MASTER_NODE_COUNT,
                state=NodeStates.RUNNING)
        master_list = self.neo.getMasterProcessList()

        # Stop the cluster (so we can start processes manually)
        self.neo.killMasters()

        # Restart admin to make sure it knows all masters.
        admin, = self.neo.getAdminProcessList()
        admin.kill()
        admin.wait()
        admin.start()

        # Start the first master.
        first_master = master_list[0]
        first_master.start()
        first_master_uuid = first_master.getUUID()
        # Check that the master node we started elected itself.
        self.neo.expectPrimary(first_master_uuid, timeout=30)
        # Check that no other node is known as running.
        self.assertEqual(len(self.neo.getMasterList(
            state=NodeStates.RUNNING)), 1)

        # Start a second master.
        second_master = master_list[1]
        # Check that the second master is known as being down.
        self.assertEqual(self.neo.getMasterNodeState(second_master.getUUID()),
                None)
        second_master.start()
        # Check that the second master is running under his known UUID.
        self.neo.expectMasterState(second_master.getUUID(),
                NodeStates.RUNNING)
        # Check that the primary master didn't change.
        self.assertEqual(self.neo.getPrimary(), first_master_uuid)

        # Start a third master.
        third_master = master_list[2]
        # Check that the third master is known as being down.
        self.assertEqual(self.neo.getMasterNodeState(third_master.getUUID()),
                None)
        third_master.start()
        # Check that the third master is running under his known UUID.
        self.neo.expectMasterState(third_master.getUUID(),
                NodeStates.RUNNING)
        # Check that the primary master didn't change.
        self.assertEqual(self.neo.getPrimary(), first_master_uuid)

def test_suite():
    return unittest.makeSuite(MasterTests)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")

