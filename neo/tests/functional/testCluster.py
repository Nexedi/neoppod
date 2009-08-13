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

import unittest
from neo.tests.functional import NEOCluster, NEOFunctionalTest
from neo import protocol

class ClusterTests(NEOFunctionalTest):

    def setUp(self):
        NEOFunctionalTest.setUp(self)
        self.neo = None

    def tearDown(self):
        if self.neo is not None:
            self.neo.stop()

    def testClusterBreaks(self):
        self.neo = NEOCluster(['test_neo1'], port_base=20000, 
                master_node_count=1, temp_dir=self.getTempDirectory())
        neoctl = self.neo.getNEOCTL()
        self.neo.setupDB()
        self.neo.start()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(number=0)
        self.neo.killStorage()
        self.neo.expectClusterVeryfing()

    def testClusterBreaksWithTwoNodes(self):
        self.neo = NEOCluster(['test_neo1', 'test_neo2'], port_base=20000,
                 partitions=2, master_node_count=1, replicas=0,
                 temp_dir=self.getTempDirectory())
        neoctl = self.neo.getNEOCTL()
        self.neo.setupDB()
        self.neo.start()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(number=0)
        self.neo.killStorage()
        self.neo.expectClusterVeryfing()

    def testClusterDoesntBreakWithTwoNodesOneReplica(self):
        self.neo = NEOCluster(['test_neo1', 'test_neo2'], port_base=20000,
                         partitions=2, replicas=1, master_node_count=1,
                         temp_dir=self.getTempDirectory())
        neoctl = self.neo.getNEOCTL()
        self.neo.setupDB()
        self.neo.start()
        self.neo.expectClusterRunning()
        self.neo.expectOudatedCells(number=0)
        self.neo.killStorage()
        self.neo.expectClusterRunning()

def test_suite():
    return unittest.makeSuite(ClusterTests)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")

