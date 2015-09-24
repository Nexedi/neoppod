#!/usr/bin/env python
#
# neomaster - run a master node of NEO
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

from neo.lib import logging
from neo.lib.config import getServerOptionParser, ConfigurationManager

parser = getServerOptionParser()
parser.add_option('-u', '--uuid', help='the node UUID (testing purpose)')
parser.add_option('-r', '--replicas', help = 'replicas number')
parser.add_option('-p', '--partitions', help = 'partitions number')
parser.add_option('-A', '--autostart',
    help='minimum number of pending storage nodes to automatically start'
         ' new cluster (to avoid unwanted recreation of the cluster,'
         ' this should be the total number of storage nodes)')
parser.add_option('-C', '--upstream-cluster',
    help='the name of cluster to backup')
parser.add_option('-M', '--upstream-masters',
    help='list of master nodes in cluster to backup')

defaults = dict(
    bind = '127.0.0.1:10000',
    masters = '',
    replicas = 0,
    partitions = 100,
)

def main(args=None):
    # build configuration dict from command line options
    (options, args) = parser.parse_args(args=args)
    config = ConfigurationManager(defaults, options, 'master')

    # setup custom logging
    logging.setup(config.getLogfile())

    # and then, load and run the application
    from neo.master.app import Application
    app = Application(config)
    app.run()

