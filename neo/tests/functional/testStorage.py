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

import ZODB
import unittest
import transaction
from Persistence import Persistent

from neo.tests.functional import NEOCluster
from neo.client.Storage import Storage as NEOStorage
from neo import protocol

class PObject(Persistent):
    
    def __init__(self, value):
        self.value = value


OBJECT_NUMBER = 100

class StorageTests(unittest.TestCase):

    def setUp(self):
        self.neo = None

    def tearDown(self):
        if self.neo is not None:
            self.neo.stop()

    def queryCount(self, db, query):
        db.query(query)
        result = db.store_result().fetch_row()[0][0]
        return result

    def __setup(self, storage_number=2, pending_number=0, replicas=1, partitions=10):
        # create a neo cluster
        self.neo = NEOCluster(['test_neo%d' % i for i in xrange(storage_number)],
            port_base=20000, 
            master_node_count=2,
            partitions=10, replicas=replicas,
        )
        self.neo.setupDB()
        # too many pending storage nodes requested
        assert pending_number <= storage_number
        storage_processes  = self.neo.getStorageProcessList()
        start_storage_number = len(storage_processes) - pending_number
        # return a tuple of storage processes lists
        started_processes = storage_processes[:start_storage_number]
        stopped_processes = storage_processes[start_storage_number:]
        self.neo.start(except_storages=stopped_processes)
        return (started_processes, stopped_processes)

    def __populate(self):
        db, conn = self.neo.getZODBConnection()
        root = conn.root()
        for i in xrange(OBJECT_NUMBER):
            root[i] = PObject(i) 
        transaction.commit()
        conn.close()
        db.close()

    def __checkDatabase(self, db_name):
        db = self.neo.getSQLConnection(db_name)
        # wait for the sql transaction to be commited
        def callback(last_try):
            object_number = self.queryCount(db, 'select count(*) from obj')
            return object_number == OBJECT_NUMBER + 2, object_number
        self.neo.expectCondition(callback)
        # no more temporarily objects
        t_objects = self.queryCount(db, 'select count(*) from tobj')
        self.assertEqual(t_objects, 0)
        # One revision per object and two for the root, before and after
        revisions = self.queryCount(db, 'select count(*) from obj')
        self.assertEqual(revisions, OBJECT_NUMBER + 2)
        # One object more for the root 
        query = 'select count(*) from (select * from obj group by oid) as t'
        objects = self.queryCount(db, query)
        self.assertEqual(objects, OBJECT_NUMBER + 1)

    def __checkReplicationDone(self):
        # wait for replication to finish
        def expect_all_storages(last_try):
            storage_number = len(self.neo.getStorageNodeList())
            return storage_number == len(self.neo.db_list), storage_number
        self.neo.expectCondition(expect_all_storages, timeout=10)
        self.neo.expectOudatedCells(number=0, timeout=10)
        # check databases
        for db_name in self.neo.db_list:
            self.__checkDatabase(db_name)

        # check storages state
        storage_list = self.neo.getStorageNodeList(protocol.RUNNING_STATE)
        self.assertEqual(len(storage_list), 2)

    def __expectRunning(self, process):
        self.neo.expectStorageState(process.getUUID(), protocol.RUNNING_STATE)

    def __expectPending(self, process):
        self.neo.expectStorageState(process.getUUID(), protocol.PENDING_STATE)
    
    def __expectUnavailable(self, process):
        self.neo.expectStorageState(process.getUUID(),
                protocol.TEMPORARILY_DOWN_STATE)

    def __expectNotKnown(self, process):
        def expected_storage_not_known(last_try):
            storage_list = self.neo.getStorageNodeList()
            for storage in storage_list:
                if storage[2] == process.getUUID():
                    return False, storage
            return True, None
        self.neo.expectCondition(expected_storage_not_known)
    
    def testReplicationWithoutBreak(self):
        """ Start a cluster with two storage, one replicas, the two databasqes
        must have the same content """

        # populate the cluster then check the databases
        self.__setup(storage_number=2, replicas=1)
        self.__populate()
        self.__checkReplicationDone()

    def testNewNodesInPendingState(self):
        """ Check that new storage nodes are set as pending, the cluster remains
        running """

        # start with the first storage
        processes = self.__setup(storage_number=3, replicas=1, pending_number=2)
        started, stopped = processes
        self.__expectRunning(started[0])
        self.neo.expectClusterRunning()

        # start the second then the third
        stopped[0].start()
        self.__expectPending(stopped[0])
        self.neo.expectClusterRunning()
        stopped[1].start()
        self.__expectPending(stopped[1])
        self.neo.expectClusterRunning()

    def testReplicationWithNewStorage(self):
        """ create a cluster with one storage, populate it, add a new storage
        then check the database content to ensure the replication process is
        well done """

        # populate one storage
        processes = self.__setup(storage_number=2, replicas=1, pending_number=1,
                partitions=10)
        started, stopped = processes
        self.__populate()
        self.neo.expectClusterRunning()
        self.neo.expectAssignedCells(started[0].getUUID(), number=10)

        # start the second
        stopped[0].start()
        self.__expectPending(stopped[0])
        self.neo.expectClusterRunning()

        # add it to the partition table
        self.neo.neoctl.enableStorageList([stopped[0].getUUID()])
        self.__expectRunning(stopped[0])
        self.neo.expectAssignedCells(stopped[0].getUUID(), number=10)
        self.neo.expectClusterRunning()

        # wait for replication to finish then check 
        self.__checkReplicationDone()
        self.neo.expectClusterRunning()

    def testOudatedCellsOnDownStorage(self):
        """ Check that the storage cells are set as oudated when the node is
        down, the cluster remains up since there is a replica """

        # populate the two storages
        (started, _) = self.__setup(storage_number=2, replicas=1)
        self.__populate()
        self.__checkReplicationDone()
        self.neo.expectClusterRunning()

        # stop one storage and check outdated cells
        started[0].stop()
        self.neo.expectOudatedCells(number=10)
        self.neo.expectClusterRunning()

    def testVerificationTriggered(self):
        """ Check that the verification stage is executed when a storage node
        required to be operationnal is lost, and the cluster come back in
        running state when the storage is up again """

        # start neo with one storages
        (started, _) = self.__setup(replicas=0, storage_number=1)
        self.__expectRunning(started[0])

        # stop it, the cluster must switch to verification
        started[0].stop()
        self.__expectUnavailable(started[0])
        self.neo.expectClusterVeryfing()

        # restart it, the cluster must come back to running state
        started[0].start()
        self.__expectRunning(started[0])
        self.neo.expectClusterRunning()

    def testSequentialStorageKill(self):
        """ Check that the cluster remains running until the last storage node
        died when all are replicas """

        # start neo with three storages / two replicas
        (started, _) = self.__setup(replicas=2, storage_number=3, partitions=10)
        self.__expectRunning(started[0])
        self.__expectRunning(started[1])
        self.__expectRunning(started[2])
        self.neo.expectOudatedCells(number=0)
        self.neo.expectClusterRunning()

        # stop one storage, cluster must remains running
        started[0].stop()
        self.__expectUnavailable(started[0])
        self.__expectRunning(started[1])
        self.__expectRunning(started[2])
        self.neo.expectOudatedCells(number=10)
        self.neo.expectClusterRunning()

        # stop a second storage, cluster is still running
        started[1].stop()
        self.__expectUnavailable(started[0])
        self.__expectUnavailable(started[1])
        self.__expectRunning(started[2])
        self.neo.expectOudatedCells(number=20)
        self.neo.expectClusterRunning()

        # stop the last, cluster died
        started[2].stop()
        self.__expectUnavailable(started[0])
        self.__expectUnavailable(started[1])
        self.__expectUnavailable(started[2])
        self.neo.expectOudatedCells(number=20)
        self.neo.expectClusterVeryfing()

    def testConflictingStorageRejected(self):
        """ Check that a storage coming after the recovery process with the same
        UUID as another already running is refused """

        # start with one storage
        (started, stopped) = self.__setup(storage_number=2, pending_number=1)
        self.__expectRunning(started[0])
        self.neo.expectClusterRunning()

        # start the second with the same UUID as the first
        stopped[0].setUUID(started[0].getUUID())
        stopped[0].start()

        # check the first and the cluster are still running
        self.__expectRunning(started[0])
        self.neo.expectClusterRunning()

        # XXX: should wait for the storage rejection

        # check that no node were added
        storage_number = len(self.neo.getStorageNodeList())
        self.assertEqual(storage_number, 1)

    def testPartitionTableReorganizedWithNewStorage(self):
        """ Check if the partition change when adding a new storage to a cluster
        with one storage and no replicas """

        # start with one storage and no replicas
        (started, stopped) = self.__setup(storage_number=2, pending_number=1, 
            partitions=10, replicas=0)
        self.__expectRunning(started[0])
        self.neo.expectClusterRunning()
        self.neo.expectAssignedCells(started[0].getUUID(), 10)

        # start the second and add it to the partition table
        stopped[0].start()
        self.__expectPending(stopped[0])
        self.neo.neoctl.enableStorageList([stopped[0].getUUID()])
        self.__expectRunning(stopped[0])
        self.neo.expectClusterRunning()

        # the partition table must change, each node should be assigned to 
        # five partitions
        self.neo.expectAssignedCells(started[0].getUUID(), 5) 
        self.neo.expectAssignedCells(stopped[0].getUUID(), 5) 

    def testPartitionTableReorganizedAfterDrop(self):
        """ Check that the partition change when dropping a replicas from a
        cluster with two storages """

        # start with two storage / one replicas
        (started, stopped) = self.__setup(storage_number=2, replicas=1,
                partitions=10, pending_number=0)
        self.__expectRunning(started[0])
        self.__expectRunning(started[1])
        self.neo.expectAssignedCells(started[0].getUUID(), 10)
        self.neo.expectAssignedCells(started[1].getUUID(), 10)

        # kill one storage, it should be set as unavailable
        started[0].stop()
        self.__expectUnavailable(started[0])
        self.__expectRunning(started[1])
        # and the partition table must not change
        self.neo.expectAssignedCells(started[0].getUUID(), 10)
        self.neo.expectAssignedCells(started[1].getUUID(), 10)
    
        # ask neoctl to drop it
        self.neo.neoctl.dropNode(started[0].getUUID())
        self.__expectNotKnown(started[0])
        self.neo.expectAssignedCells(started[0].getUUID(), 0)
        self.neo.expectAssignedCells(started[1].getUUID(), 10)

    def testReplicationThenRunningWithReplicas(self):
        """ Add a replicas to a cluster, wait for the replication to finish,
        shutdown the first storage then check the new storage content """

        # start with one storage 
        (started, stopped) = self.__setup(storage_number=2, replicas=1,
                pending_number=1, partitions=10)
        self.__expectRunning(started[0])
        self.__expectNotKnown(stopped[0])

        # populate the cluster with some data
        self.__populate()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(number=0)
        self.neo.expectAssignedCells(started[0].getUUID(), 10)
        self.__checkDatabase(self.neo.db_list[0])

        # add a second storage
        stopped[0].start()
        self.__expectPending(stopped[0])
        self.neo.neoctl.enableStorageList([stopped[0].getUUID()])
        self.__expectRunning(stopped[0])
        self.neo.expectClusterRunning()
        self.neo.expectAssignedCells(started[0].getUUID(), 10)
        self.neo.expectAssignedCells(stopped[0].getUUID(), 10)

        # wait for replication to finish
        self.neo.expectOudatedCells(number=0)
        self.neo.expectClusterRunning()
        self.__checkReplicationDone()

        # kill the first storage
        started[0].stop()
        self.__expectUnavailable(started[0])
        self.neo.expectOudatedCells(number=10)
        self.neo.expectAssignedCells(started[0].getUUID(), 10)
        self.neo.expectAssignedCells(stopped[0].getUUID(), 10)
        self.neo.expectClusterRunning()
        self.__checkDatabase(self.neo.db_list[0])

        # drop it from partition table
        self.neo.neoctl.dropNode(started[0].getUUID())
        self.__expectNotKnown(started[0])
        self.__expectRunning(stopped[0])
        self.neo.expectAssignedCells(started[0].getUUID(), 0)
        self.neo.expectAssignedCells(stopped[0].getUUID(), 10)
        self.__checkDatabase(self.neo.db_list[1])

        

if __name__ == "__main__":
    unittest.main()
