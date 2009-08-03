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
        return self._killMaster(primary=True, all=all)

    def getMasterNodeState(self, uuid):
        node_list = self.neoctl.getNodeList(protocol.MASTER_NODE_TYPE)
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
        time.sleep(1)
        # Check node state has changed.
        self.assertEqual(self.getMasterNodeState(uuid), protocol.TEMPORARILY_DOWN_STATE)

    def testStoppingPrimaryMaster(self):
        killed_uuid_list = self.killPrimaryMaster()
        # Test sanity check.
        self.assertEqual(len(killed_uuid_list), 1)
        uuid = killed_uuid_list[0]
        # Leave some time for the other master to elect a new primary.
        time.sleep(1)
        # Check that a primary master arised.
        # (raises if admin node is not connected to a primary master)
        new_uuid = self.neoctl.getPrimaryMaster()
        # Check that the uuid really changed.
        self.assertNotEqual(new_uuid, uuid)

def test_suite():
    return unittest.makeSuite(MasterTests)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")

