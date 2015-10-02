#!/usr/bin/env python
#
# neoadmin - run an administrator node of NEO
#
# Copyright (C) 2009-2015  Nexedi SA
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
from neo.lib.config import getOptionParser
from neo.lib.util import parseNodeAddress

parser = getOptionParser()
parser.add_option('-a', '--address', help = 'specify the address (ip:port) ' \
    'of an admin node', default = '127.0.0.1:9999')
parser.add_option('--handler', help = 'specify the connection handler')

def main(args=None):
    (options, args) = parser.parse_args(args=args)
    if options.address is not None:
        address = parseNodeAddress(options.address, 9999)
    else:
        address = ('127.0.0.1', 9999)

    if options.logfile:
        # Contrary to daemons, we log everything to disk automatically
        # because a user using -l option here:
        # - is certainly debugging an issue and wants everything,
        # - would not have to time to send SIGRTMIN before neoctl exits.
        logging.backlog(None)
        logging.setup(options.logfile)
    from neo.neoctl.app import Application

    ssl = options.ca, options.cert, options.key
    print Application(address, ssl=ssl if any(ssl) else None).execute(args)

