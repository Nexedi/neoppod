#!/usr/bin/env python
#
# neostorage - run a storage node of NEO
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
parser.add_option('-u', '--uuid', help='specify an UUID to use for this ' \
                  'process. Previously assigned UUID takes precedence (ie ' \
                  'you should always use -R with this switch)')
parser.add_option('-R', '--reset', action = 'store_true',
                  help = 'remove an existing database if any')
parser.add_option('-a', '--adapter', help = 'database adapter to use')
parser.add_option('-d', '--database', help = 'database connections string')
parser.add_option('-e', '--engine', help = 'database engine')
parser.add_option('-w', '--wait', help='seconds to wait for backend to be '
    'available, before erroring-out (-1 = infinite)', type='float', default=0)

defaults = dict(
    bind = '127.0.0.1',
    masters = '127.0.0.1:10000',
    adapter = 'MySQL',
)

def main(args=None):
    # TODO: Forbid using "reset" along with any unneeded argument.
    #       "reset" is too dangerous to let user a chance of accidentally
    #       letting it slip through in a long option list.
    #       We should drop support configation files to make such check useful.
    (options, args) = parser.parse_args(args=args)
    config = ConfigurationManager(defaults, options, 'storage')

    # setup custom logging
    logging.setup(config.getLogfile())

    # and then, load and run the application
    from neo.storage.app import Application
    app = Application(config)
    if not config.getReset():
        app.run()
