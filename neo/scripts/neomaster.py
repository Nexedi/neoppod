#!/usr/bin/env python
#
# neomaster - run a master node of NEO
#
# Copyright (C) 2006-2019  Nexedi SA
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
    from neo.master.app import Application
    config = Application.option_parser.parse(args)

    # setup custom logging
    logging.setup(config.get('logfile'))

    # and then, load and run the application
    app = Application(config)
    app.run()
