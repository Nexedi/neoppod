#
# Copyright (C) 2006-2012  Nexedi SA
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

from ConfigParser import SafeConfigParser, NoOptionError
from . import util
from .util import parseNodeAddress

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
                try:
                    value = self.parser.get(self.section, key)
                except NoOptionError:
                    pass
        if value is None and not optional:
            raise RuntimeError("Option '%s' is undefined'" % (key, ))
        return value

    def getMasters(self):
        """ Get the master node list except itself """
        masters = self.__get('masters')
        # load master node list except itself
        return util.parseMasterList(masters, except_node=self.getBind())

    def getBind(self):
        """ Get the address to bind to """
        bind = self.__get('bind')
        return parseNodeAddress(bind, 0)

    def getDatabase(self):
        return self.__get('database')

    def getWait(self):
        return self.__get('wait')

    def getDynamicMasterList(self):
        return self.__get('dynamic_master_list', optional=True)

    def getAdapter(self):
        return self.__get('adapter')

    def getCluster(self):
        cluster = self.__get('cluster')
        assert cluster != '', "Cluster name must be non-empty"
        return cluster

    def getReplicas(self):
        return int(self.__get('replicas'))

    def getPartitions(self):
        return int(self.__get('partitions'))

    def getReset(self):
        # only from command line
        return self.argument_list.get('reset', False)

    def getUUID(self):
        # only from command line
        uuid = self.argument_list.get('uuid', None)
        if uuid:
            return int(uuid)

    def getUpstreamCluster(self):
        return self.__get('upstream_cluster', True)

    def getUpstreamMasters(self):
        return util.parseMasterList(self.__get('upstream_masters'))
