#
# Copyright (C) 2006-2010  Nexedi SA
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

from ConfigParser import SafeConfigParser
from neo.lib import util
from neo.lib.util import parseNodeAddress

class ConfigurationManager(object):
    """
    Configuration manager that load options from a configuration file and
    command line arguments
    """

    def __init__(self, defaults, config_file, section, argument_list):
        self.defaults = defaults
        self.argument_list = argument_list
        self.parser = None
        if config_file is not None:
            self.parser = SafeConfigParser(defaults)
            self.parser.read(config_file)
        self.section = section

    def __get(self, key, optional=False):
        value = self.argument_list.get(key)
        if value is None:
            if self.parser is None:
                value = self.defaults.get(key)
            else:
                value = self.parser.get(self.section, key)
        if value is None and not optional:
            raise RuntimeError("Option '%s' is undefined'" % (key, ))
        return value

    def getMasters(self):
        """ Get the master node list except itself """
        masters = self.__get('masters')
        # lod master node list except itself
        return util.parseMasterList(masters, except_node=self.getBind())

    def getBind(self):
        """ Get the address to bind to """
        bind = self.__get('bind')
        return parseNodeAddress(bind, 0)

    def getDatabase(self):
        return self.__get('database')

    def getAdapter(self):
        return self.__get('adapter')

    def getCluster(self):
        cluster = self.__get('cluster')
        assert cluster != '', "Cluster name must be non-empty"
        return cluster

    def getName(self):
        return self.__get('name')

    def getReplicas(self):
        return int(self.__get('replicas'))

    def getPartitions(self):
        return int(self.__get('partitions'))

    def getReset(self):
        # only from command line
        return self.argument_list.get('reset', False)

    def getUUID(self):
        # only from command line
        return util.bin(self.argument_list.get('uuid', None))

