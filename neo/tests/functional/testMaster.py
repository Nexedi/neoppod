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

DELAY_SAFETY_MARGIN = 5
MASTER_NODE_COUNT = 3

neo = NEOCluster([], port_base=20000, master_node_count=MASTER_NODE_COUNT)

class MasterTests(unittest.TestCase):

    def setUp(self):
        neo.stop()
        neo.start()
        self.storage = neo.getStorage()
        self.neoctl = neo.getNEOCTL()

    def tearDown(self):
        neo.stop()

    def expectCondition(self, condition, timeout, delay):
        end = time() + timeout + DELAY_SAFETY_MARGIN
        opaque = None
        opaque_history = []
        while time() < end:
            reached, opaque = condition(opaque)
            if reached:
                break
            else:
                opaque_history.append(opaque)
                sleep(delay)
        else:
          raise AssertionError, 'Timeout while expecting condition. ' \
                                'History: %s' % (opaque_history, )

    def expectAllMasters(self, state=None, timeout=0, delay=1):
        def callback(last_try):
            current_try = len(neo.getMasterNodeList(state=state))
            if last_try is not None and current_try < last_try:
                raise AssertionError, 'Regression: %s became %s' % \
                    (last_try, current_try)
            return (current_try == MASTER_NODE_COUNT, current_try)
        self.expectCondition(callback, timeout, delay)

    def expectMasterState(self, uuid, state, timeout=0, delay=1):
        if not isinstance(state, (tuple, list)):
            state = (state, )
        def callback(last_try):
            current_try = neo.getMasterNodeState(uuid)
            return current_try in state, current_try
        self.expectCondition(callback, timeout, delay)

    def expectPrimaryMaster(self, uuid=None, timeout=0, delay=1):
        def callback(last_try):
            current_try = neo.getPrimaryMaster()
            if None not in (uuid, current_try) and uuid != current_try:
                raise AssertionError, 'An unexpected primary arised: %r, ' \
                    'expected %r' % (dump(current_try), dump(uuid))
            return uuid is None or uuid == current_try, current_try
        self.expectCondition(callback, timeout, delay)

    def testStoppingSecondaryMaster(self):
        # Wait for masters to stabilize
        self.expectAllMasters()

        # Kill
        killed_uuid_list = neo.killSecondaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Check node state has changed.
        self.expectMasterState(uuid, None)

    def testStoppingPrimaryMasterWithTwoSecondaries(self):
        # Wait for masters to stabilize
        self.expectAllMasters()

        # Kill
        killed_uuid_list = neo.killPrimaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Check the state of the primary we just killed
        self.expectMasterState(uuid, (None, protocol.UNKNOWN_STATE))
        self.assertEqual(self.getPrimaryMaster(), None)
        # Check that a primary master arised.
        self.expectPrimaryMaster(timeout=10)
        # Check that the uuid really changed.
        new_uuid = self.getPrimaryMaster()
        self.assertNotEqual(new_uuid, uuid)

    def testStoppingPrimaryMasterWithOneSecondary(self):
        self.expectAllMasters(state=protocol.RUNNING_STATE)

        # Kill one secondary master.
        killed_uuid_list = neo.killSecondaryMaster()
        # Test sanity checks.
        self.assertEqual(len(killed_uuid_list), 1)
        self.expectMasterState(killed_uuid_list[0], None)
        self.assertEqual(len(neo.getMasterNodeList()), 2)

        killed_uuid_list = neo.killPrimaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Check the state of the primary we just killed
        self.expectMasterState(uuid, (None, protocol.UNKNOWN_STATE))
        self.assertEqual(self.getPrimaryMaster(), None)
        # Check that a primary master arised.
        self.expectPrimaryMaster(timeout=10)
        # Check that the uuid really changed.
        new_uuid = self.getPrimaryMaster()
        self.assertNotEqual(new_uuid, uuid)

    def testMasterSequentialStart(self):
        self.expectAllMasters(state=protocol.RUNNING_STATE)
        master_list = neo.getMasterProcessList()

        # Stop the cluster (so we can start processes manually)
        neo.killMasters()

        # Start the first master.
        first_master = master_list[0]
        first_master.start()
        first_master_uuid = first_master.getUUID()
        # Check that the master node we started elected itself.
        self.expectPrimaryMaster(first_master_uuid, timeout=60)

        # Start a second master.
        second_master = master_list[1]
        second_master.start()
        # Check that the second master is running under his known UUID.
        self.expectMasterState(second_master.getUUID(), protocol.RUNNING_STATE)
        # Check that the primary master didn't change.
        self.assertEqual(neo.getPrimaryMaster(), first_master_uuid)

        # Start a third master.
        third_master = master_list[2]
        third_master.start()
        # Check that the third master is running under his known UUID.
        self.expectMasterState(third_master.getUUID(), protocol.RUNNING_STATE)
        # Check that the primary master didn't change.
        self.assertEqual(self.getPrimaryMaster(), first_master_uuid)

def test_suite():
    return unittest.makeSuite(MasterTests)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")

