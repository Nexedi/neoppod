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

import unittest, os
from mock import Mock
from neo.protocol import RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, INVALID_UUID
from neo.config import ConfigurationManager
from tempfile import mkstemp


class ConfigurationManagerTests(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def getNewUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def test_01_configuration_manager(self):      
        # initialisation
        #create a fake configuration file
        config_file_text = """# Default parameters.
[DEFAULT]
# The list of master nodes.
master_nodes: 127.0.0.2:10010 127.0.0.2
# The number of replicas.
replicas: 25
# The number of partitions.
partitions: 243125
# The name of this cluster.
name: unittest
# The user name for the database.
user: neotest
# The password for the database.
password: neotest
connector : SocketTestConnector
# The first master.
[mastertest]
server: 127.0.0.1:15010

# The first storage.
[storage1]
database: neotest1
server: 127.0.0.5

# The second storage.
[storage2]
database: neotest2
server: 127.0.0.1:15021
"""
        tmp_id, self.tmp_path = mkstemp()
        tmp_file = os.fdopen(tmp_id, "w+b")
        tmp_file.write(config_file_text)
        tmp_file.close()
        config = ConfigurationManager(self.tmp_path, "mastertest")
        self.assertNotEqual(config.parser, None)
        self.assertEqual(config.section, "mastertest")
        # some values will be get from default config into class
        self.assertEqual(config.getDatabase(), "test")
        self.assertEqual(config.getUser(), "neotest")
        self.assertEqual(config.getPassword(), "neotest")
        self.assertEqual(config.getServer(), ("127.0.0.1", 15010))
        self.assertEqual(config.getReplicas(), 25)
        self.assertEqual(config.getPartitions(), 243125)
        self.assertEqual(config.getConnector(), "SocketTestConnector")
        self.assertEqual(config.getName(), "unittest")
        self.assertEqual(len(config.getMasterNodeList()), 2)
        node_list = config.getMasterNodeList()
        self.failUnless(("127.0.0.2", 10010) in node_list)
        self.failUnless(("127.0.0.2", 10100) in node_list)

        # test with a storage where no port is defined, must get the default one
        config = ConfigurationManager(self.tmp_path, "storage1")
        self.assertNotEqual(config.parser, None)
        self.assertEqual(config.section, "storage1")
        # some values will be get from default config into class
        self.assertEqual(config.getDatabase(), "neotest1")
        self.assertEqual(config.getUser(), "neotest")
        self.assertEqual(config.getPassword(), "neotest")
        self.assertEqual(config.getServer(), ("127.0.0.5", 10100))
        self.assertEqual(config.getReplicas(), 25)
        self.assertEqual(config.getPartitions(), 243125)
        self.assertEqual(config.getConnector(), "SocketTestConnector")
        self.assertEqual(config.getName(), "unittest")
        self.assertEqual(len(config.getMasterNodeList()), 2)
        node_list = config.getMasterNodeList()
        self.failUnless(("127.0.0.2", 10010) in node_list)
        self.failUnless(("127.0.0.2", 10100) in node_list)
        
        
if __name__ == '__main__':
    unittest.main()

