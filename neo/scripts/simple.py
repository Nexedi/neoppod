#!/usr/bin/env python
#
# Copyright (C) 2011-2015  Nexedi SA
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

import inspect, random, signal, sys
from logging import getLogger, INFO
from optparse import OptionParser
from neo.lib import logging
from neo.tests import functional
logging.backlog()
del logging.default_root_handler.handle

def main():
    args, _, _, defaults = inspect.getargspec(functional.NEOCluster.__init__)
    option_list = zip(args[-len(defaults):], defaults)
    parser = OptionParser(usage="%prog [options] [db...]",
        description="Quickly setup a simple NEO cluster for testing purpose.")
    parser.add_option('--seed', help="settings like node ports/uuids and"
        " cluster name are random: pass any string to initialize the RNG")
    defaults = {}
    for option, default in sorted(option_list):
        kw = {}
        if type(default) is bool:
            kw['action'] = "store_true"
            defaults[option] = False
        elif default is not None:
            defaults[option] = default
            if isinstance(default, int):
                kw['type'] = "int"
        parser.add_option('--' + option, **kw)
    parser.set_defaults(**defaults)
    options, args = parser.parse_args()
    if options.seed:
        functional.random = random.Random(options.seed)
    getLogger().setLevel(INFO)
    cluster = functional.NEOCluster(args, **{x: getattr(options, x)
                                             for x, _ in option_list})
    try:
        cluster.run()
        logging.info("Cluster running ...")
        cluster.waitAll()
    finally:
        cluster.stop()

if __name__ == "__main__":
    main()
