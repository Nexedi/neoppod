#
# Copyright (C) 2006-2015  Nexedi SA
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

import os
from optparse import OptionParser
from ConfigParser import SafeConfigParser, NoOptionError
from . import util
from .util import parseNodeAddress


def getOptionParser():
    parser = OptionParser()
    parser.add_option('-l', '--logfile',
        help='log debugging information to specified SQLite DB')
    parser.add_option('--ca', help='certificate authority in PEM format')
    parser.add_option('--cert', help='certificate in PEM format')
    parser.add_option('--key', help='private key in PEM format')
    return parser

def getServerOptionParser():
    parser = getOptionParser()
    parser.add_option('-f', '--file', help='specify a configuration file')
    parser.add_option('-s', '--section', help='specify a configuration section')
    parser.add_option('-c', '--cluster', help='the cluster name')
    parser.add_option('-m', '--masters', help='master node list')
    parser.add_option('-b', '--bind', help='the local address to bind to')
    parser.add_option('-D', '--dynamic-master-list',
        help='path of the file containing dynamic master node list')
    return parser


class ConfigurationManager(object):
    """
    Configuration manager that load options from a configuration file and
    command line arguments
    """

    def __init__(self, defaults, options, section):
        self.argument_list = options = {k: v
            for k, v in options.__dict__.iteritems()
            if v is not None}
        self.defaults = defaults
        config_file = options.pop('file', None)
        if config_file:
            self.parser = SafeConfigParser(defaults)
            self.parser.read(config_file)
        else:
            self.parser = None
        self.section = options.pop('section', section)

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

    def __getPath(self, *args, **kw):
        path = self.__get(*args, **kw)
        if path:
            return os.path.expanduser(path)

    def getLogfile(self):
        return self.__getPath('logfile', True)

    def getSSL(self):
        r = [self.__getPath(key, True) for key in ('ca', 'cert', 'key')]
        if any(r):
            return r

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

    def getEngine(self):
        return self.__get('engine', True)

    def getWait(self):
        # BUG
        return self.__get('wait')

    def getDynamicMasterList(self):
        return self.__getPath('dynamic_master_list', optional=True)

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

    def getAutostart(self):
        n = self.__get('autostart', True)
        if n:
            return int(n)
