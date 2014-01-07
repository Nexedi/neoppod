#!/usr/bin/env python
#
# neomaster - run a master node of NEO
#
# Copyright (C) 2006-2014  Nexedi SA
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

from optparse import OptionParser
from neo.lib import logging
from neo.lib.config import ConfigurationManager

parser = OptionParser()
parser.add_option('-f', '--file', help = 'specify a configuration file')
parser.add_option('-s', '--section', help = 'specify a configuration section')
parser.add_option('-u', '--uuid', help='the node UUID (testing purpose)')
parser.add_option('-b', '--bind', help = 'the local address to bind to')
parser.add_option('-c', '--cluster', help = 'the cluster name')
parser.add_option('-m', '--masters', help = 'master node list')
parser.add_option('-r', '--replicas', help = 'replicas number')
parser.add_option('-p', '--partitions', help = 'partitions number')
parser.add_option('-l', '--logfile', help = 'specify a logging file')
parser.add_option('-D', '--dynamic-master-list', help='path of the file '
    'containing dynamic master node list')

defaults = dict(
    bind = '127.0.0.1:10000',
    masters = '',
    replicas = 0,
    partitions = 100,
)

def main(args=None):
    # build configuration dict from command line options
    (options, args) = parser.parse_args(args=args)
    arguments = dict(
        uuid = options.uuid or None,
        bind = options.bind,
        cluster = options.cluster,
        masters = options.masters,
        replicas = options.replicas,
        partitions = options.partitions,
    )
    config = ConfigurationManager(
            defaults,
            options.file,
            options.section or 'master',
            arguments,
    )

    # setup custom logging
    logging.setup(options.logfile)

    # and then, load and run the application
    from neo.master.app import Application
    app = Application(config)
    app.run()

