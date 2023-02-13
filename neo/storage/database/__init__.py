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

LOG_QUERIES = False

def useMySQLdb():
    import platform
    py = platform.python_implementation() == 'PyPy'
    try:
        if py:
            import pymysql
        else:
            import MySQLdb
    except ImportError:
        return py
    return not py

class getAdapterKlass(object):

    def __new__(cls, name):
        try:
            m = getattr(cls, name or 'MySQL')
        except AttributeError:
            raise DatabaseFailure('Cannot find a database adapter <%s>' % name)
        return m()

    @staticmethod
    def Importer():
        from .importer import ImporterDatabaseManager as DM
        return DM

    @classmethod
    def MySQL(cls, MySQLdb=None):
        if MySQLdb is not None:
            global useMySQLdb
            useMySQLdb = lambda: MySQLdb
        from .mysql import binding_name, MySQLDatabaseManager as DM
        assert hasattr(cls, binding_name)
        return DM

    MySQLdb = classmethod(lambda cls: cls.MySQL(True))
    PyMySQL = classmethod(lambda cls: cls.MySQL(False))

    @staticmethod
    def SQLite():
        from .sqlite import SQLiteDatabaseManager as DM
        return DM

DATABASE_MANAGERS = tuple(sorted(
    x for x in dir(getAdapterKlass) if not x.startswith('_')))

def buildDatabaseManager(name, args=(), kw={}):
    return getAdapterKlass(name)(*args, **kw)

class DatabaseFailure(Exception):
    pass
