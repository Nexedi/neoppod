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
import MySQLdb
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
        storage_number = ['test_neo%d' % i for i in xrange(storage_number)]
        self.neo = NEOCluster(storage_number, port_base=20000, 
            master_node_count=2,
            partitions=10, replicas=replicas,
        )
        self.neo.setupDB()
        # too many pending storage nodes requested
        if pending_number > storage_number:
            pending_number = storage_number
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
        db = MySQLdb.connect(db=db_name, user='test')
        # wait for the sql transaction to be commited
        def callback(last_try):
            object_number = self.queryCount(db, 'select count(*) from obj')
            return object_number == OBJECT_NUMBER + 2, object_number
        self.neo.expectCondition(callback, 0, 1)
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
    
    def testReplicationWithoutBreak(self):

        # populate the cluster then check the databases
        self.__setup(storage_number=2, replicas=1)
        self.__populate()
        self.__checkReplicationDone()

    def testNewNodesInPendingState(self):

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

        # populate one storage
        processes = self.__setup(storage_number=2, replicas=1, pending_number=1)
        started, stopped = processes
        self.__populate()
        self.neo.expectClusterRunning()

        # start the second
        stopped[0].start()
        self.__expectPending(stopped[0])
        self.neo.expectClusterRunning()

        # add it to the partition table
        self.neo.neoctl.enableStorageList([stopped[0].getUUID()])
        self.__expectRunning(stopped[0])
        self.neo.expectClusterRunning()

        # wait for replication to finish then check 
        self.__checkReplicationDone()
        self.neo.expectClusterRunning()

    def testOudatedCellsOnDownStorage(self):

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
    

if __name__ == "__main__":
    unittest.main()
