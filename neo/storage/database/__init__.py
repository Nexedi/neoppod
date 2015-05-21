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

LOG_QUERIES = False

from neo.lib.exception import DatabaseFailure
from .manager import DatabaseManager

DATABASE_MANAGER_DICT = {
    'Importer': 'importer.ImporterDatabaseManager',
    'MySQL': 'mysqldb.MySQLDatabaseManager',
    'SQLite': 'sqlite.SQLiteDatabaseManager',
}

def getAdapterKlass(name):
    try:
        module, name = DATABASE_MANAGER_DICT[name or 'MySQL'].split('.')
    except KeyError:
        raise DatabaseFailure('Cannot find a database adapter <%s>' % name)
    module = getattr(__import__(__name__, fromlist=[module], level=1), module)
    return getattr(module, name)

def buildDatabaseManager(name, args=(), kw={}):
    return getAdapterKlass(name)(*args, **kw)
