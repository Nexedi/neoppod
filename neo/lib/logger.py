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

# WARNING: Log rotating should not be implemented here.
#          SQLite does not access database only by file descriptor,
#          and an OperationalError exception would be raised if a log is emitted
#          between a rename and a reopen.
#          Fortunately, SQLite allow multiple process to access the same DB,
#          so an external tool should be able to dump and empty tables.

from collections import deque
from functools import wraps
from logging import getLogger, Formatter, Logger, StreamHandler, \
    DEBUG, WARNING
from time import time
from traceback import format_exception
import bz2, inspect, neo, os, signal, sqlite3, sys, threading

# Stats for storage node of matrix test (py2.7:SQLite)
RECORD_SIZE = ( 234360832 # extra memory used
              - 16777264  # sum of raw data ('msg' attribute)
              ) // 187509 # number of records

FMT = ('%(asctime)s %(levelname)-9s %(name)-10s'
       ' [%(module)14s:%(lineno)3d] \n%(message)s')

class _Formatter(Formatter):

    def formatTime(self, record, datefmt=None):
        return Formatter.formatTime(self, record,
           '%Y-%m-%d %H:%M:%S') + '.%04d' % (record.msecs * 10)

    def format(self, record):
        lines = iter(Formatter.format(self, record).splitlines())
        prefix = lines.next()
        return '\n'.join(prefix + line for line in lines)


class PacketRecord(object):

    args = None
    levelno = DEBUG
    __init__ = property(lambda self: self.__dict__.update)


class NEOLogger(Logger):

    default_root_handler = StreamHandler()
    default_root_handler.setFormatter(_Formatter(FMT))

    def __init__(self):
        Logger.__init__(self, None)
        self.parent = root = getLogger()
        if not root.handlers:
            root.addHandler(self.default_root_handler)
        self._db = None
        self._record_queue = deque()
        self._record_size = 0
        self._async = set()
        l = threading.Lock()
        self._acquire = l.acquire
        release = l.release
        def _release():
            try:
                while self._async:
                    self._async.pop()(self)
            finally:
                release()
        self._release = _release
        self.backlog()

    def __enter__(self):
        self._acquire()
        return self._db

    def __exit__(self, t, v, tb):
        self._release()

    def __async(wrapped):
        def wrapper(self):
            self._async.add(wrapped)
            if self._acquire(0):
                self._release()
        return wraps(wrapped)(wrapper)

    @__async
    def reopen(self):
        if self._db is None:
            return
        q = self._db.execute
        if not q("SELECT id FROM packet LIMIT 1").fetchone():
            q("DROP TABLE protocol")
            # DROP TABLE already replaced previous data with zeros,
            # so VACUUM is not really useful. But here, it should be free.
            q("VACUUM")
        self._setup(q("PRAGMA database_list").fetchone()[2])

    @__async
    def flush(self):
        if self._db is None:
            return
        try:
            for r in self._record_queue:
                self._emit(r)
        finally:
            # Always commit, to not lose any record that we could emit.
            self.commit()
        self._record_queue.clear()
        self._record_size = 0

    def commit(self):
        try:
            self._db.commit()
        except sqlite3.OperationalError, e:
            x = e.args[0]
            if x == 'database is locked':
                sys.stderr.write('%s: retrying to emit log...' % x)
                while e.args[0] == x:
                    try:
                        self._db.commit()
                    except sqlite3.OperationalError, e:
                        continue
                    sys.stderr.write(' ok\n')
                    return
            raise

    def backlog(self, max_size=1<<24, max_packet=None):
        with self:
            self._max_packet = max_packet
            self._max_size = max_size
            if max_size is None:
                self.flush()
            else:
                q = self._record_queue
                while max_size < self._record_size:
                    self._record_size -= RECORD_SIZE + len(q.popleft().msg)

    def _setup(self, filename=None, reset=False):
        from . import protocol as p
        global uuid_str
        uuid_str = p.uuid_str
        if self._db is not None:
            self._db.close()
            if not filename:
                self._db = None
                self._record_queue.clear()
                self._record_size = 0
                return
        if filename:
            self._db = sqlite3.connect(filename, check_same_thread=False)
            q = self._db.execute
            if self._max_size is None:
                q("PRAGMA synchronous = OFF")
            if 1: # Not only when logging everything,
                  # but also for interoperability with logrotate.
                q("PRAGMA journal_mode = MEMORY")
            if reset:
                for t in 'log', 'packet':
                    q('DROP TABLE IF EXISTS ' + t)
            q("""CREATE TABLE IF NOT EXISTS log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date REAL NOT NULL,
                    name TEXT,
                    level INTEGER NOT NULL,
                    pathname TEXT,
                    lineno INTEGER,
                    msg TEXT)
              """)
            q("""CREATE INDEX IF NOT EXISTS _log_i1 ON log(date)""")
            q("""CREATE TABLE IF NOT EXISTS packet (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date REAL NOT NULL,
                    name TEXT,
                    msg_id INTEGER NOT NULL,
                    code INTEGER NOT NULL,
                    peer TEXT NOT NULL,
                    body BLOB)
              """)
            q("""CREATE INDEX IF NOT EXISTS _packet_i1 ON packet(date)""")
            q("""CREATE TABLE IF NOT EXISTS protocol (
                    date REAL PRIMARY KEY NOT NULL,
                    text BLOB NOT NULL)
              """)
            with open(inspect.getsourcefile(p)) as p:
                p = buffer(bz2.compress(p.read()))
            for t, in q("SELECT text FROM protocol ORDER BY date DESC"):
                if p == t:
                    break
            else:
                try:
                    t = self._record_queue[0].created
                except IndexError:
                    t = time()
                with self._db:
                    q("INSERT INTO protocol VALUES (?,?)", (t, p))

    def setup(self, filename=None, reset=False):
        with self:
            self._setup(filename, reset)
    __del__ = setup

    def isEnabledFor(self, level):
        return True

    def _emit(self, r):
        if type(r) is PacketRecord:
            ip, port = r.addr
            peer = '%s %s (%s:%u)' % ('>' if r.outgoing else '<',
                                      uuid_str(r.uuid), ip, port)
            msg = r.msg
            if msg is not None:
                msg = buffer(msg)
            self._db.execute("INSERT INTO packet VALUES (NULL,?,?,?,?,?,?)",
                (r.created, r._name, r.msg_id, r.code, peer, msg))
        else:
            pathname = os.path.relpath(r.pathname, *neo.__path__)
            self._db.execute("INSERT INTO log VALUES (NULL,?,?,?,?,?,?)",
                (r.created, r._name, r.levelno, pathname, r.lineno, r.msg))

    def _queue(self, record):
        record._name = self.name and str(self.name)
        self._acquire()
        try:
            if self._max_size is None:
                self._emit(record)
                self.commit()
            else:
                self._record_size += RECORD_SIZE + len(record.msg)
                q = self._record_queue
                q.append(record)
                if record.levelno < WARNING:
                    while self._max_size < self._record_size:
                        self._record_size -= RECORD_SIZE + len(q.popleft().msg)
                else:
                    self.flush()
        finally:
            self._release()

    def callHandlers(self, record):
        if self._db is not None:
            record.msg = record.getMessage()
            record.args = None
            if record.exc_info:
                record.msg += '\n' + ''.join(
                    format_exception(*record.exc_info)).strip()
                record.exc_info = None
            self._queue(record)
        if Logger.isEnabledFor(self, record.levelno):
            record.name = self.name or 'NEO'
            self.parent.callHandlers(record)

    def packet(self, connection, packet, outgoing):
        if self._db is not None:
            body = packet._body
            if self._max_packet and self._max_packet < len(body):
                body = None
            self._queue(PacketRecord(
                created=time(),
                msg_id=packet._id,
                code=packet._code,
                outgoing=outgoing,
                uuid=connection.getUUID(),
                addr=connection.getAddress(),
                msg=body))


logging = NEOLogger()
signal.signal(signal.SIGRTMIN, lambda signum, frame: logging.flush())
signal.signal(signal.SIGRTMIN+1, lambda signum, frame: logging.reopen())
