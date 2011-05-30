#!/usr/bin/env python
##############################################################################
#
# Copyright (c) 2011 Nexedi SARL and Contributors. All Rights Reserved.
#                    Julien Muchembled <jm@nexedi.com>
#
# WARNING: This program as such is intended to be used by professional
# programmers who take the whole responsibility of assessing all potential
# consequences resulting from its eventual inadequacies and bugs
# End users who are looking for a ready-to-use solution with commercial
# guarantees and support are strongly advised to contract a Free Software
# Service Company
#
# This program is Free Software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
#
##############################################################################

import inspect, random, signal, sys
from optparse import OptionParser
from neo.lib import logger, logging
from neo.tests import functional

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
    if options.verbose:
        logger.PACKET_LOGGER.enable(True)
    if options.seed:
        functional.random = random.Random(options.seed)
    cluster = functional.NEOCluster(args, **dict((x, getattr(options, x))
                                                 for x, _ in option_list))
    try:
        cluster.start()
        logging.info("Cluster running ...")
        signal.pause()
    finally:
        cluster.stop()

if __name__ == "__main__":
    sys.exit(main())
