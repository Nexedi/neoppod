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
import transaction
from neo.lib.protocol import NodeStates

from . import NEOCluster, NEOFunctionalTest

class ClusterTests(NEOFunctionalTest):

    def _tearDown(self, success):
        if hasattr(self, "neo"):
            self.neo.stop()
            del self.neo
        NEOFunctionalTest._tearDown(self, success)

    def testClusterStartup(self):
        neo = self.neo = NEOCluster(['test_neo1', 'test_neo2'], replicas=1,
                         temp_dir=self.getTempDirectory())
        neoctl = neo.neoctl
        neo.run()
        # Runing a new cluster doesn't exit Recovery state.
        s1, s2 = neo.getStorageProcessList()
        neo.expectPending(s1)
        neo.expectPending(s2)
        neo.expectClusterRecovering()
        # When allowing cluster to exit Recovery, it reaches Running state and
        # all present storage nodes reach running state.
        neoctl.startCluster()
        neo.expectRunning(s1)
        neo.expectRunning(s2)
        neo.expectClusterRunning()
        # Re-running cluster with a missing storage doesn't exit Recovery
        # state.
        neo.stop()
        neo.run(except_storages=(s2, ))
        neo.expectPending(s1)
        neo.expectUnknown(s2)
        neo.expectClusterRecovering()
        # Starting missing storage allows cluster to exit Recovery without
        # neoctl action.
        s2.start()
        neo.expectRunning(s1)
        neo.expectRunning(s2)
        neo.expectClusterRunning()
        # Re-running cluster with a missing storage and allowing startup exits
        # recovery.
        neo.stop()
        neo.run(except_storages=(s2, ))
        neo.expectPending(s1)
        neo.expectUnknown(s2)
        neo.expectClusterRecovering()
        neoctl.startCluster()
        neo.expectRunning(s1)
        neo.expectUnknown(s2)
        neo.expectClusterRunning()

    def testClusterBreaks(self):
        self.neo = NEOCluster(['test_neo1'],
                master_count=1, temp_dir=self.getTempDirectory())
        self.neo.setupDB()
        self.neo.start()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(number=0)
        self.neo.killStorage()
        self.neo.expectClusterVerifying()

    def testClusterBreaksWithTwoNodes(self):
        self.neo = NEOCluster(['test_neo1', 'test_neo2'],
                 partitions=2, master_count=1, replicas=0,
                 temp_dir=self.getTempDirectory())
        self.neo.setupDB()
        self.neo.start()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(number=0)
        self.neo.killStorage()
        self.neo.expectClusterVerifying()

    def testClusterDoesntBreakWithTwoNodesOneReplica(self):
        self.neo = NEOCluster(['test_neo1', 'test_neo2'],
                         partitions=2, replicas=1, master_count=1,
                         temp_dir=self.getTempDirectory())
        self.neo.setupDB()
        self.neo.start()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(number=0)
        self.neo.killStorage()
        self.neo.expectClusterRunning()

    def testElectionWithManyMasters(self):
        MASTER_COUNT = 20
        self.neo = NEOCluster(['test_neo1', 'test_neo2'],
            partitions=10, replicas=0, master_count=MASTER_COUNT,
            temp_dir=self.getTempDirectory())
        self.neo.start()
        self.neo.expectClusterRunning()
        self.neo.expectAllMasters(MASTER_COUNT, NodeStates.RUNNING)
        self.neo.expectOudatedCells(0)

    def testLeavingOperationalStateDropClientNodes(self):
        """
            Check that client nodes are dropped where the cluster leaves the
            operational state.
        """
        # start a cluster
        self.neo = NEOCluster(['test_neo1'], replicas=0,
            temp_dir=self.getTempDirectory())
        self.neo.start()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(0)
        # connect a client a check it's known
        db, conn = self.neo.getZODBConnection()
        self.assertEqual(len(self.neo.getClientlist()), 1)
        # drop the storage, the cluster is no more operational...
        self.neo.getStorageProcessList()[0].stop()
        self.neo.expectClusterVerifying()
        # ...and the client gets disconnected
        self.assertEqual(len(self.neo.getClientlist()), 0)
        # restart storage so that the cluster is operational again
        self.neo.getStorageProcessList()[0].start()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(0)
        # and reconnect the client, there must be only one known by the admin
        conn.root()['plop'] = 1
        transaction.commit()
        self.assertEqual(len(self.neo.getClientlist()), 1)

    def testStorageLostDuringRecovery(self):
        """
            Check that admin node receive notifications of storage
            connection and disconnection during recovery
        """
        self.neo = NEOCluster(['test_neo%d' % i for i in xrange(2)],
            master_count=1, partitions=10, replicas=1,
            temp_dir=self.getTempDirectory(), clear_databases=True,
        )
        storages  = self.neo.getStorageProcessList()
        self.neo.run(except_storages=storages)
        self.neo.expectStorageNotKnown(storages[0])
        self.neo.expectStorageNotKnown(storages[1])
        storages[0].start()
        self.neo.expectPending(storages[0])
        self.neo.expectStorageNotKnown(storages[1])
        storages[1].start()
        self.neo.expectPending(storages[0])
        self.neo.expectPending(storages[1])
        storages[0].stop()
        self.neo.expectUnavailable(storages[0])
        self.neo.expectPending(storages[1])
        storages[1].stop()
        self.neo.expectUnavailable(storages[0])
        self.neo.expectUnavailable(storages[1])

def test_suite():
    return unittest.makeSuite(ClusterTests)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")

