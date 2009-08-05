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
        pass

    def tearDown(self):
        pass

    def queryCount(self, db, query):
        db.query(query)
        result = db.store_result().fetch_row()[0][0]
        return result

    def populate(self, neo):
        db, conn = neo.getConnection()
        root = conn.root()
        for i in xrange(OBJECT_NUMBER):
            root[i] = PObject(i) 
        transaction.commit()
        conn.close()
        db.close()

    def checkDatabase(self, neo, db_name):
        db = MySQLdb.connect(db=db_name, user='test')
        # wait for the sql transaction to be commited
        def callback(last_try):
            object_number = self.queryCount(db, 'select count(*) from obj')
            return object_number == OBJECT_NUMBER + 2, last_try
        neo.expectCondition(callback, 0, 1)
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

    def __checkReplicationDone(self, neo, databases):
        # wait for replication to finish
        neo.expectNoOudatedCells(timeout=10)
        # check databases
        for db_name in databases:
            self.checkDatabase(neo, db_name)

        # check storages state
        self.assertEqual(len(neo.getStorageNodeList(protocol.RUNNING_STATE)), 2)
    
    def testReplicationWithoutBreak(self):

        # create a neo cluster
        databases = ['test_neo1', 'test_neo2']
        neo = NEOCluster(databases, port_base=20000, 
            master_node_count=2,
            partitions=10, replicas=1,
        )
        neo.setupDB()
        neo.start()

        # populate the cluster and check
        self.populate(neo)
        self.__checkReplicationDone(neo, databases)

    def testReplicationWithNewStorage(self):

        # create a neo cluster
        databases = ['test_neo1', 'test_neo2']
        neo = NEOCluster(databases, port_base=20000, 
            master_node_count=2,
            partitions=10, replicas=1,
        )
        neo.setupDB()

        # populate one storage
        new_storage = neo.getStorageProcessList()[-1]
        neo.start(except_storages=[new_storage])
        self.populate(neo)

        # start the second
        new_storage.start()
        neo.expectStorageState(new_storage.getUUID(), protocol.PENDING_STATE)

        # add it to the partition table
        neo.neoctl.enableStorageList([new_storage.getUUID()])
        neo.expectStorageState(new_storage.getUUID(), protocol.RUNNING_STATE)

        # wait for replication to finish then check 
        self.__checkReplicationDone(neo, databases)
    

if __name__ == "__main__":
    unittest.main()
