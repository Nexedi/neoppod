#
# Copyright (C) 2006-2009  Nexedi SA
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

from ConfigParser import SafeConfigParser
import logging

class ConfigurationManager:
    """This class provides support for parsing a configuration file."""

    # The default values for a config file.
    default_config_dict = {'database' : 'test',
                           'user' : 'test',
                           'password' : None,
                           'server' : '127.0.0.1',
                           'master_nodes' : '',
                           'replicas' : '1',
                           'partitions' : '1009',
                           'name' : 'main'}

    # The default port to which master nodes listen when none is specified.
    default_master_port = 10100

    def __init__(self, file, section):
        parser = SafeConfigParser(self.default_config_dict)
        logging.debug('reading a configuration from %s', file)
        parser.read(file)
        self.parser = parser
        self.section = section

    def __getitem__(self, key):
        return self.parser.get(self.section, key)

    def getDatabase(self):
        return self['database']

    def getUser(self):
        return self['user']

    def getPassword(self):
        return self['password']

    def getServer(self):
        server = self['server']
        if ':' in server:
            ip_address, port = server.split(':')
            port = int(port)
        else:
            ip_address = server
            port = self.default_master_port
        return ip_address, port

    def getReplicas(self):
        return int(self['replicas'])

    def getPartitions(self):
        return int(self['partitions'])

    def getConnector(self):
        return str(self['connector'])

    def getName(self):
        return self['name']

    def getMasterNodeList(self):
        master_nodes = self['master_nodes']
        master_node_list = []
        # A list of master nodes separated by whitespace.
        for node in master_nodes.split():
            if not node:
                continue
            if ':' in node:
                ip_address, port = node.split(':')
                port = int(port)
            else:
                ip_address = node
                port = self.default_master_port
            master_node_list.append((ip_address, port))
        return master_node_list
        
