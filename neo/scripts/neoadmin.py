#!/usr/bin/env python
#
# neoadmin - run an administrator  node of NEO
#
# Copyright (C) 2009-2017  Nexedi SA
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
parser.add_option('-u', '--uuid', help='specify an UUID to use for this ' \
                  'process')

defaults = dict(
    bind = '127.0.0.1:9999',
    masters = '127.0.0.1:10000',
)

def main(args=None):
    # build configuration dict from command line options
    (options, args) = parser.parse_args(args=args)
    config = ConfigurationManager(defaults, options, 'admin')

    # setup custom logging
    logging.setup(config.getLogfile())

    # and then, load and run the application
    from neo.admin.app import Application
    app = Application(config)
    app.run()

