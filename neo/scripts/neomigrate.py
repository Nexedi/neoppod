#!/usr/bin/env python
#
# neomigrate - import/export data between NEO and a FileStorage
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

from __future__ import print_function
import time
import os
from neo.lib.app import buildOptionParser

import_warning = (
    "WARNING: This is not the recommended way to import data to NEO:"
    " you should use the Importer backend instead.\n"
    "NEO also does not implement IStorageRestoreable interface, which"
    " means that undo information is not preserved when using this tool:"
    " conflict resolution could happen when undoing an old transaction."
)

@buildOptionParser
class NEOMigrate(object):

    from neo.lib.config import OptionList

    @classmethod
    def _buildOptionParser(cls):
        parser = cls.option_parser
        parser.description = "NEO <-> FileStorage conversion tool"
        parser('c', 'cluster', required=True, help='the NEO cluster name')
        parser.bool('q', 'quiet', help='print nothing to standard output')
        parser.argument('source', help='the source database')
        parser.argument('destination', help='the destination database')

    def __init__(self, config):
        self.name = config.pop('cluster')
        self.source = config.pop('source')
        self.destination = config.pop('destination')
        self.quiet = config.pop('quiet', False)

        from ZODB.FileStorage import FileStorage
        from neo.client.Storage import Storage as NEOStorage
        if os.path.exists(self.source):
            if not self.quiet:
                print(import_warning)
            self.src = FileStorage(file_name=self.source, read_only=True)
            self.dst = NEOStorage(master_nodes=self.destination, name=self.name,
                                  **config)
        else:
            self.src = NEOStorage(master_nodes=self.source, name=self.name,
                                  read_only=True, **config)
            self.dst = FileStorage(file_name=self.destination)

    def run(self):
        if not self.quiet:
            print("Migrating from %s to %s" % (self.source, self.destination))
            start = time.time()
        self.dst.copyTransactionsFrom(self.src)
        if not self.quiet:
            elapsed = time.time() - start
            print("Migration done in %3.5f" % elapsed)


def main(args=None):
    config = NEOMigrate.option_parser.parse(args)
    NEOMigrate(config).run()
