#
# Copyright (c) 2011 Nexedi SARL and Contributors. All Rights Reserved.
#                    Julien Muchembled <jm@nexedi.com>
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

from persistent import Persistent
from neo.lib.protocol import NodeStates
from neo.tests.threaded import NEOCluster, NEOThreadedTest
from neo.client.pool import CELL_CONNECTED, CELL_GOOD

class PObject(Persistent):
    pass


class Test(NEOThreadedTest):

    def test_commit(self):
        cluster = NEOCluster()
        cluster.start(1)
        try:
            t, c = cluster.getTransaction()
            c.root()['foo'] = PObject()
            t.commit()
        finally:
            cluster.stop()

    def test_notifyNodeInformation(self):
        # translated from MasterNotificationsHandlerTests
        # (neo.tests.client.testMasterHandler)
        cluster = NEOCluster()
        try:
            cluster.start(1)
            cluster.client.setPoll(0)
            storage, = cluster.client.nm.getStorageList()
            conn = storage.getConnection()
            self.assertFalse(conn.isClosed())
            getCellSortKey = cluster.client.cp.getCellSortKey
            self.assertEqual(getCellSortKey(storage), CELL_CONNECTED)
            cluster.neoctl.dropNode(cluster.storage.uuid)
            self.assertFalse(cluster.client.nm.getStorageList())
            self.assertTrue(conn.isClosed())
            self.assertEqual(getCellSortKey(storage), CELL_GOOD)
            # XXX: the test originally checked that 'unregister' method
            #      was called (even if it's useless in this case),
            #      but we would need an API to do that easily.
            self.assertFalse(cluster.client.dispatcher.registered(conn))
        finally:
            cluster.stop()

    def testRestartWithMissingStorage(self, fast_startup=False):
        # translated from neo.tests.functional.testStorage.StorageTest
        cluster = NEOCluster(replicas=1, partitions=10)
        s1, s2 = cluster.storage_list
        try:
            cluster.start()
            self.assertEqual([], cluster.getOudatedCells())
        finally:
            cluster.stop()
        # restart it with one storage only
        cluster.reset()
        try:
            cluster.start(storage_list=(s1,), fast_startup=fast_startup)
            self.assertEqual(NodeStates.UNKNOWN, cluster.getNodeState(s2))
        finally:
            cluster.stop()

    def testRestartWithMissingStorageFastStartup(self):
        self.testRestartWithMissingStorage(True)
