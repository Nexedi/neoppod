#!/usr/bin/env python
#
# neomaster - run a master node of NEO
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

from neo.lib.config import getOptionParser
import time
import os

# register options
parser = getOptionParser()
parser.add_option('-s', '--source', help='the source database')
parser.add_option('-d', '--destination', help='the destination database')
parser.add_option('-c', '--cluster', help='the NEO cluster name')

def main(args=None):
    # parse options
    (options, args) = parser.parse_args(args=args)
    source = options.source or None
    destination = options.destination or None
    cluster = options.cluster or None

    # check options
    if source is None or destination is None:
        raise RuntimeError('Source and destination databases must be supplied')
    if cluster is None:
        raise RuntimeError('The NEO cluster name must be supplied')

    # open storages
    from ZODB.FileStorage import FileStorage
    from neo.client.Storage import Storage as NEOStorage
    if os.path.exists(source):
        print("WARNING: This is not the recommended way to import data to NEO:"
              " you should use Imported backend instead.\n"
              "NEO also does not implement IStorageRestoreable interface,"
              " which means that undo information is not preserved when using"
              " this tool: conflict resolution could happen when undoing an"
              " old transaction.")
        src = FileStorage(file_name=source, read_only=True)
        dst = NEOStorage(master_nodes=destination, name=cluster,
                         logfile=options.logfile)
    else:
        src = NEOStorage(master_nodes=source, name=cluster,
                         logfile=options.logfile, read_only=True)
        dst = FileStorage(file_name=destination)

    # do the job
    print "Migrating from %s to %s" % (source, destination)
    start = time.time()
    dst.copyTransactionsFrom(src)
    elapsed = time.time() - start
    print "Migration done in %3.5f" % (elapsed, )

