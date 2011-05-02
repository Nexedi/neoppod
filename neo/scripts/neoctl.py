# neoadmin - run an administrator node of NEO
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

import sys
from optparse import OptionParser
from neo.lib import setupLog
from neo.lib.util import parseNodeAddress

parser = OptionParser()
parser.add_option('-v', '--verbose', action = 'store_true',
                  help = 'print verbose messages')
parser.add_option('-a', '--address', help = 'specify the address (ip:port) ' \
    'of an admin node', default = '127.0.0.1:9999')
parser.add_option('--handler', help = 'specify the connection handler')

def main(args=None):
    (options, args) = parser.parse_args(args=args)
    if options.address is not None:
        address = parseNodeAddress(options.address, 9999)
    else:
        address = ('127.0.0.1', 9999)

    setupLog('NEOCTL', options.verbose)
    from neo.neoctl.app import Application

    print Application(address).execute(args)

