#!/usr/bin/env python
#
# neoctl - command-line interface to an administrator node of NEO
#
# Copyright (C) 2009-2019  Nexedi SA
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

def main(args=None):
    from neo.neoctl.neoctl import NeoCTL
    config = NeoCTL.option_parser.parse(args)

    logfile = config.get('logfile')
    if logfile:
        # Contrary to daemons, we log everything to disk automatically
        # because a user using -l option here:
        # - is certainly debugging an issue and wants everything,
        # - would not have to time to send SIGRTMIN before neoctl exits.
        logging.backlog(None)
        logging.setup(logfile)

    from neo.neoctl.app import Application
    app = Application(config['address'], ssl=config.get('ssl'))
    r = app.execute(config['cmd'])
    if r is not None:
        print r
