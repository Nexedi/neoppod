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

import os
import time
import ZODB
import MySQLdb
import unittest
import tempfile
import transaction
from ZODB.FileStorage import FileStorage
from Persistence import Persistent

from neo.tests.functional import NEOCluster
from neo.client.Storage import Storage as NEOStorage

class PObject(Persistent):
    
    def __init__(self, value):
        self.value = value


class ImportExportTests(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='neo_import_export_')
        print "using the temp directory %s" % self.temp_dir

    def tearDown(self):
        pass

    def queryCount(self, db, query):
        db.query(query)
        result = db.store_result().fetch_row()[0][0]
        return result
    
    def testReplicationWithoutBreak(self):

        OBJECT_NUMBER = 100

        # create a neo cluster
        databases = ['test_neo1', 'test_neo2']
        neo = NEOCluster(databases, port_base=20000, 
            master_node_count=2,
            partitions=10, replicas=1,
        )
        neo.setupDB()
        neo.start()

        # create a neo storage
        args = {'connector': 'SocketConnector', 'name': neo.cluster_name}
        storage = NEOStorage(master_nodes=neo.master_nodes, **args)
        db = ZODB.DB(storage=storage)
        
        # populate the cluster
        conn = db.open()
        root = conn.root()
        for i in xrange(OBJECT_NUMBER):
            root[i] = PObject(i) 
        transaction.commit()
        conn.close()
        storage.close()
        
        # XXX: replace this sleep by a callback as done in testMaster
        time.sleep(1)

        # check databases
        for index, db_name in enumerate(databases):
            db = MySQLdb.connect(db=db_name, user='test')
            # One revision per object and two for the root, before and after
            revisions = self.queryCount(db, 'select count(*) from obj')
            self.assertEqual(revisions, OBJECT_NUMBER + 2)
            # One object more for the root 
            query = 'select count(*) from (select * from obj group by oid) as t'
            objects = self.queryCount(db, query)
            self.assertEqual(objects, OBJECT_NUMBER + 1)

    

if __name__ == "__main__":
    unittest.main()
