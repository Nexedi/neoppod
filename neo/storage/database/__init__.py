#
# Copyright (C) 2006-2009  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from neo.exception import DatabaseFailure
from neo.storage.database.manager import DatabaseManager
from neo.storage.database.mysqldb import MySQLDatabaseManager

DATABASE_MANAGER_DICT = {
    'MySQL': MySQLDatabaseManager,
}

def buildDatabaseManager(name, config):
    adapter_klass = DATABASE_MANAGER_DICT.get(name, None)
    if adapter_klass is None:
        raise DatabaseFailure('Cannot find a database adapter <%s>' % name)
    return adapter_klass(config)

