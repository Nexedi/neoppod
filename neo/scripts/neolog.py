#!/usr/bin/env python
#
# neolog - read a NEO log
#
# Copyright (C) 2012-2015  Nexedi SA
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

import bz2, gzip, errno, optparse, os, signal, sqlite3, sys, time
from bisect import insort
from logging import getLevelName

comp_dict = dict(bz2=bz2.BZ2File, gz=gzip.GzipFile)

class Log(object):

    _log_id = _packet_id = -1
    _protocol_date = None

    def __init__(self, db_path, decode_all=False, date_format=None,
                                filter_from=None):
        self._date_format = '%F %T' if date_format is None else date_format
        self._decode_all = decode_all
        self._filter_from = filter_from
        name = os.path.basename(db_path)
        try:
            name, ext = name.rsplit(os.extsep, 1)
            ZipFile = comp_dict[ext]
        except (KeyError, ValueError):
            # WKRD: Python does not support URI so we can't open in read-only
            #       mode. See http://bugs.python.org/issue13773
            os.stat(db_path) # do not create empty DB if file is missing
            self._db = sqlite3.connect(db_path)
        else:
            import shutil, tempfile
            with tempfile.NamedTemporaryFile() as f:
                shutil.copyfileobj(ZipFile(db_path), f)
                self._db = sqlite3.connect(f.name)
            name = name.rsplit(os.extsep, 1)[0]
        self._default_name = name

    def __iter__(self):
        db =  self._db
        try:
            db.execute("BEGIN")
            yield
            nl = "SELECT * FROM log WHERE id>?"
            np = "SELECT * FROM packet WHERE id>?"
            date = self._filter_from
            if date:
                date = " AND date>=%f" % date
                nl += date
                np += date
            nl = db.execute(nl, (self._log_id,))
            np = db.execute(np, (self._packet_id,))
            try:
                p = np.next()
                self._reload(p[1])
            except StopIteration:
                p = None
            for self._log_id, date, name, level, pathname, lineno, msg in nl:
                while p and p[1] < date:
                    yield self._packet(*p)
                    p = np.fetchone()
                yield date, name, getLevelName(level), msg.splitlines()
            if p:
                yield self._packet(*p)
                for p in np:
                    yield self._packet(*p)
        finally:
            db.rollback()

    def _reload(self, date):
        q = self._db.execute
        date, text = q("SELECT * FROM protocol WHERE date<=?"
                       " ORDER BY date DESC", (date,)).next()
        if self._protocol_date == date:
            return
        self._protocol_date = date
        g = {}
        exec bz2.decompress(text) in g
        for x in 'uuid_str', 'Packets', 'PacketMalformedError':
            setattr(self, x, g[x])
        try:
            self._next_protocol, = q("SELECT date FROM protocol WHERE date>?",
                                     (date,)).next()
        except StopIteration:
            self._next_protocol = float('inf')

    def _emit(self, date, name, levelname, msg_list):
        prefix = self._date_format
        if prefix:
            d = int(date)
            prefix = '%s.%04u ' % (time.strftime(prefix, time.localtime(d)),
                                   int((date - d) * 10000))
        prefix += '%-9s %-10s ' % (levelname, name or self._default_name)
        for msg in msg_list:
            print prefix + msg

    def _packet(self, id, date, name, msg_id, code, peer, body):
        self._packet_id = id
        if self._next_protocol <= date:
            self._reload(date)
        try:
            p = self.Packets[code]
        except KeyError:
            Packets[code] = p = type('UnknownPacket[%u]' % code, (object,), {})
        msg = ['#0x%04x %-30s %s' % (msg_id, p.__name__, peer)]
        if body is not None:
            logger = getattr(self, p.handler_method_name, None)
            if logger or self._decode_all:
                p = p()
                p._id = msg_id
                p._body = body
                try:
                    args = p.decode()
                except self.PacketMalformedError:
                    msg.append("Can't decode packet")
                else:
                    if logger:
                        msg += logger(*args)
                    elif args:
                        msg = '%s \t| %r' % (msg[0], args),
        return date, name, 'PACKET', msg

    def error(self, code, message):
        return "%s (%s)" % (code, message),

    def notifyNodeInformation(self, node_list):
        node_list.sort(key=lambda x: x[2])
        node_list = [(self.uuid_str(uuid), str(node_type),
                      '%s:%u' % address if address else '?', state)
                     for node_type, address, uuid, state in node_list]
        if node_list:
            t = ' ! %%%us | %%%us | %%%us | %%s' % (
                max(len(x[0]) for x in node_list),
                max(len(x[1]) for x in node_list),
                max(len(x[2]) for x in node_list))
            return map(t.__mod__, node_list)
        return ()


def emit_many(log_list):
    log_list = [(log, iter(log).next) for log in log_list]
    for x in log_list: # try to start all transactions at the same time
        x[1]()
    event_list = []
    for log, next in log_list:
        try:
            event = next()
        except StopIteration:
            continue
        event_list.append((-event[0], next, log._emit, event))
    if event_list:
        event_list.sort()
        while True:
            key, next, emit, event = event_list.pop()
            try:
                next_date = - event_list[-1][0]
            except IndexError:
                next_date = float('inf')
            try:
                while event[0] <= next_date:
                    emit(*event)
                    event = next()
            except IOError, e:
                if e.errno == errno.EPIPE:
                    sys.exit(1)
                raise
            except StopIteration:
                if not event_list:
                    break
            else:
                insort(event_list, (-event[0], next, emit, event))

def main():
    parser = optparse.OptionParser()
    parser.add_option('-a', '--all', action="store_true",
        help='decode all packets')
    parser.add_option('-d', '--date', metavar='FORMAT',
        help='custom date format, according to strftime(3)')
    parser.add_option('-f', '--follow', action="store_true",
        help='output appended data as the file grows')
    parser.add_option('-F', '--flush', action="append", type="int",
        help='with -f, tell process PID to flush logs approximately N'
              ' seconds (see -s)', metavar='PID')
    parser.add_option('-s', '--sleep-interval', type="float", default=1,
        help='with -f, sleep for approximately N seconds (default 1.0)'
              ' between iterations', metavar='N')
    parser.add_option('--from', dest='filter_from', type="float",
        help='show records more recent that timestamp N if N > 0,'
             ' or now+N if N < 0', metavar='N')
    options, args = parser.parse_args()
    if options.sleep_interval <= 0:
        parser.error("sleep_interval must be positive")
    if not args:
        parser.error("no log specified")
    filter_from = options.filter_from
    if filter_from and filter_from < 0:
        filter_from += time.time()
    log_list = [Log(db_path, options.all, options.date, filter_from)
                for db_path in args]
    if options.follow:
        try:
            pid_list = options.flush or ()
            while True:
                emit_many(log_list)
                for pid in pid_list:
                    os.kill(pid, signal.SIGRTMIN)
                time.sleep(options.sleep_interval)
        except KeyboardInterrupt:
            pass
    else:
        emit_many(log_list)

if __name__ == "__main__":
    main()
