#!/usr/bin/env python
#
# Copyright (C) 2011-2019  Nexedi SA
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

import argparse, inspect, random
from logging import getLogger, INFO, DEBUG
from neo.lib import logging
from neo.tests import functional
#logging.backlog()
logging.backlog(max_size=None)
del logging.default_root_handler.handle

def main():
    args, _, _, defaults = inspect.getargspec(functional.NEOCluster.__init__)
    option_list = zip(args[-len(defaults):], defaults)
    parser = argparse.ArgumentParser(
        description="Quickly setup a simple NEO cluster for testing purpose.")
    parser.add_argument('--seed', help="settings like node ports/uuids and"
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
                kw['type'] = int
        parser.add_argument('--' + option, **kw)
    parser.set_defaults(**defaults)
    parser.add_argument('db', nargs='+')
    args = parser.parse_args()
    if args.seed:
        functional.random = random.Random(args.seed)
    getLogger().setLevel(DEBUG)
    cluster = functional.NEOCluster(args.db, **{x: getattr(args, x)
                                                for x, _ in option_list})
    try:
        cluster.run()
        logging.info("Cluster running ...")
        cluster.waitAll()
    finally:
        cluster.stop()

if __name__ == "__main__":
    main()
