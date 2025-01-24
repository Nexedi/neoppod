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

import os
from functools import partial, wraps
from ZODB.DB import DB
from ZODB.FileStorage import FileStorage
from ZODB.tests import testZODB
from neo.storage import database as database_module
from neo.storage.database.importer import ImporterDatabaseManager
from .. import expectedFailure, getTempDirectory, Patch
from . import functional, ZODBTestCase

class NEOZODBTests(ZODBTestCase, testZODB.ZODBTests):

    def open(self):
        self._open(_db=DB(self.neo.getZODBStorage()))


class DummyImporter(ImporterDatabaseManager):

    compress = True

    def __init__(self, zodb, getAdapterKlass, name,
                 database, engine=None, wait=None):
        self._conf = conf = {
            'adapter': name,
            'database': database,
            'engine': engine,
            'wait': wait,
        }
        self.zodb = zodb
        with Patch(database_module,
                   getAdapterKlass=lambda orig, name: getAdapterKlass(name)):
            super(DummyImporter, self).__init__(None)

    _parse = startJobs = lambda *args, **kw: None


class NEOZODBImporterTests(NEOZODBTests):

    @classmethod
    def setUpClass(cls):
        super(NEOZODBImporterTests, cls).setUpClass()
        path = os.path.join(getTempDirectory(),
            "%s.%s.fs" % (cls.__module__, cls.__name__))
        DB(FileStorage(path)).close()
        cls._importer_config = ('root', {'storage': """<filestorage>
                path %s
            </filestorage>""" % path}),

    def run(self, *args, **kw):
        with Patch(database_module, getAdapterKlass=lambda *args:
                partial(DummyImporter, self._importer_config, *args)) as p:
            self._importer_patch = p
            super(ZODBTestCase, self).run(*args, **kw)

    if functional:
        def _getDatabaseManager(self):
            self._importer_patch.revert()
            return super(NEOZODBImporterTests, self)._getDatabaseManager()

    testMultipleUndoInOneTransaction = expectedFailure(IndexError)
