##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import time
import unittest
from neo.tests.functional import NEOCluster
from neo import protocol

neo = NEOCluster([], port_base=20000, master_node_count=3)

DELAY_SAFETY_MARGIN = 5

class MasterTests(unittest.TestCase):

    def setUp(self):
        neo.stop()
        neo.start()
        self.storage = neo.getStorage()
        self.neoctl = neo.getNEOCTL()

    def tearDown(self):
        neo.stop()

    def _killMaster(self, primary=False, all=False):
        killed_uuid_list = []
        primary_uuid = self.neoctl.getPrimaryMaster()
        for master in neo.getMasterProcessList():
            master_uuid = master.getUUID()
            is_primary = master_uuid == primary_uuid
            if primary and is_primary or \
               not (primary or is_primary):
                 killed_uuid_list.append(master_uuid)
                 master.kill()
                 master.wait()
                 if not all:
                     break
        return killed_uuid_list

    def killPrimaryMaster(self):
        return self._killMaster(primary=True)

    def killSecondaryMaster(self, all=False):
        return self._killMaster(primary=False, all=all)

    def killMasters(self):
        secondary_list = self.killSecondaryMaster(all=True)
        primary_list = self.killPrimaryMaster()
        return secondary_list + primary_list

    def getMasterNodeList(self):
        return self.neoctl.getNodeList(protocol.MASTER_NODE_TYPE)

    def getMasterNodeState(self, uuid):
        node_list = self.getMasterNodeList()
        for node_type, address, node_uuid, state in node_list:
            if node_uuid == uuid:
                break
        else:
            state = None
        return state

    def testStoppingSecondaryMaster(self):
        killed_uuid_list = self.killSecondaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Leave some time for the primary master to notice the disconnection.
        time.sleep(DELAY_SAFETY_MARGIN)
        # Check node state has changed.
        self.assertEqual(self.getMasterNodeState(uuid), None)

    def testStoppingPrimaryMasterWithTwoSecondaries(self):
        killed_uuid_list = self.killPrimaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Leave some time for the disconnection to be noticed by the admin
        # node.
        time.sleep(DELAY_SAFETY_MARGIN)
        # Check the state of the primary we just killed
        self.assertEqual(self.getMasterNodeState(uuid),
                         protocol.TEMPORARILY_DOWN_STATE)
        # Leave some time for the other masters to elect a new primary.
        time.sleep(10)
        # Check that a primary master arised.
        # (raises if admin node is not connected to a primary master)
        new_uuid = self.neoctl.getPrimaryMaster()
        # Check that the uuid really changed.
        self.assertNotEqual(new_uuid, uuid)

    def testStoppingPrimaryMasterWithOneSecondary(self):
        # Kill one secondary master.
        killed_uuid_list = self.killSecondaryMaster()
        # Test sanity checks.
        self.assertEqual(len(killed_uuid_list), 1)
        time.sleep(DELAY_SAFETY_MARGIN)
        self.assertEqual(self.getMasterNodeState(killed_uuid_list[0]), None)
        self.assertEqual(len(self.getMasterNodeList()), 2)

        killed_uuid_list = self.killPrimaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Leave some time for the disconnection to be noticed by the admin
        # node.
        time.sleep(DELAY_SAFETY_MARGIN)
        # Check the state of the primary we just killed
        self.assertEqual(self.getMasterNodeState(uuid),
                         protocol.TEMPORARILY_DOWN_STATE)
        # Leave some time for the other masters to elect a new primary.
        time.sleep(10)
        # Check that a primary master arised.
        # (raises if admin node is not connected to a primary master)
        new_uuid = self.neoctl.getPrimaryMaster()
        # Check that the uuid really changed.
        self.assertNotEqual(new_uuid, uuid)

    def testMasterSequentialStart(self):
        master_list = neo.getMasterProcessList()
        # Test sanity check.
        self.assertEqual(len(master_list), 3)

        # Stop the cluster (so we can start processes manually)
        self.killMasters()

        # Start the first master.
        first_master = master_list[0]
        first_master.start()
        # Leave some time for the master to elect itself without any other
        # master.
        time.sleep(60 + DELAY_SAFETY_MARGIN)
        # Check that the master node we started elected itself.
        self.assertEqual(self.neoctl.getPrimaryMaster(),
                         first_master.getUUID())
        # Check that no other master node is known as running.
        self.assertEqual(len(self.neoctl.getNodeList(
            protocol.MASTER_NODE_TYPE)), 1)

        # Start a second master.
        second_master = master_list[1]
        second_master.start()
        # Leave the second master some time to connect to the primary master.
        time.sleep(DELAY_SAFETY_MARGIN)
        # Check that the second master is running under his known UUID.
        self.assertEqual(self.getMasterNodeState(second_master.getUUID()),
                         protocol.RUNNING_STATE)
        # Check that the primary master didn't change.
        self.assertEqual(self.neoctl.getPrimaryMaster(),
                         first_master.getUUID())

        # Start a third master.
        third_master = master_list[2]
        third_master.start()
        # Leave the third master some time to connect to the primary master.
        time.sleep(DELAY_SAFETY_MARGIN)
        # Check that the third master is running under his known UUID.
        self.assertEqual(self.getMasterNodeState(third_master.getUUID()),
                         protocol.RUNNING_STATE)
        # Check that the primary master didn't change.
        self.assertEqual(self.neoctl.getPrimaryMaster(),
                         first_master.getUUID())

def test_suite():
    return unittest.makeSuite(MasterTests)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")

