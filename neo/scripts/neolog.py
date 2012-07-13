#!/usr/bin/env python
#
# neolog - read a NEO log
#
# Copyright (C) 2012  Nexedi SA
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

import bz2, logging, optparse, os, signal, sqlite3, sys, time
from binascii import b2a_hex
from logging import getLevelName

class main(object):

    _log_id = _packet_id = -1
    _protocol_date = None

    def __new__(cls):
        self = object.__new__(cls)
        parser = optparse.OptionParser()
        parser.add_option('-f', '--follow', action="store_true",
            help='output appended data as the file grows')
        parser.add_option('-s', '--sleep-interval', type="float", default=1,
            help='with -f, sleep for approximately N seconds (default 1.0)'
                 ' between iterations', metavar='N')
        parser.add_option('-F', '--flush', action="append", type="int",
            help='with -f, tell process PID to flush logs approximately N'
                 ' seconds (see -s)', metavar='PID')
        options, (db_path,) = parser.parse_args()
        if options.sleep_interval <= 0:
            parser.error("sleep_interval must be positive")
        self._default_name, _ = os.path.splitext(os.path.basename(db_path))
        self._db = sqlite3.connect(db_path)
        if options.follow:
            try:
                pid_list = options.flush or ()
                while True:
                    self._emit_many()
                    for pid in pid_list:
                        os.kill(pid, signal.SIGRTMIN)
                    time.sleep(options.sleep_interval)
            except KeyboardInterrupt:
                pass
        else:
            self._emit_many()

    def _emit_many(self):
        db =  self._db
        try:
            db.execute("BEGIN")
            nl = db.execute("SELECT * FROM log WHERE id>?",
                            (self._log_id,))
            np = db.execute("SELECT * FROM packet WHERE id>?",
                            (self._packet_id,))
            try:
                p = np.next()
                self._reload(p[1])
            except StopIteration:
                p = None
            for self._log_id, date, name, level, pathname, lineno, msg in nl:
                while p and p[1] < date:
                    self._packet(*p)
                    p = np.fetchone()
                self._emit(date, name, getLevelName(level), msg.splitlines())
            if p:
                self._packet(*p)
                for p in np:
                    self._packet(*p)
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
        d = int(date)
        prefix = '%s.%04u %-9s %-10s ' % (
            time.strftime('%F %T', time.localtime(d)),
            int((date - d) * 10000), levelname,
            name or self._default_name)
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
            try:
                logger = getattr(self, p.handler_method_name)
            except AttributeError:
                pass
            else:
                p = p()
                p._id = msg_id
                p._body = body
                try:
                    args = p.decode()
                except self.PacketMalformedError:
                    msg.append("Can't decode packet")
                else:
                    msg += logger(*args)
        self._emit(date, name, 'PACKET', msg)

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


if __name__ == "__main__":
    main()
