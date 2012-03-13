#
# Copyright (C) 2006-2012  Nexedi SA
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
from .sqlite import SQLiteDatabaseManager

DATABASE_MANAGER_DICT = {'SQLite': SQLiteDatabaseManager}

try:
    from .mysqldb import MySQLDatabaseManager
except ImportError:
    pass
else:
    DATABASE_MANAGER_DICT['MySQL'] = MySQLDatabaseManager

def buildDatabaseManager(name, args=(), kw={}):
    if name is None:
        name = DATABASE_MANAGER_DICT.keys()[0]
    adapter_klass = DATABASE_MANAGER_DICT.get(name, None)
    if adapter_klass is None:
        raise DatabaseFailure('Cannot find a database adapter <%s>' % name)
    return adapter_klass(*args, **kw)

