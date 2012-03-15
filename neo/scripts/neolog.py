#!/usr/bin/env python
#
# neostorage - run a storage node of NEO
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

import logging, os, sqlite3, sys, time
from neo.lib.protocol import Packets, PacketMalformedError
from neo.lib.util import dump

def emit(date, name, levelname, msg_list):
    d = int(date)
    prefix = '%s.%04u %-9s %-10s ' % (time.strftime('%F %T', time.localtime(d)),
                                      int((date - d) * 10000), levelname,
                                      name or default_name)
    for msg in msg_list:
        print prefix + msg

class packet(object):

    def __new__(cls, date, name, msg_id, code, peer, body):
        try:
            p = Packets[code]
        except KeyError:
            Packets[code] = p = type('UnknownPacket[%u]' % code, (object,), {})
        msg = ['#0x%04x %-30s %s' % (msg_id, p.__name__, peer)]
        if body is not None:
            try:
                logger = getattr(cls, p.handler_method_name)
            except AttributeError:
                pass
            else:
                p = p()
                p._id = msg_id
                p._body = body
                try:
                    args = p.decode()
                except PacketMalformedError:
                    msg.append("Can't decode packet")
                else:
                    msg += logger(*args)
        emit(date, name, 'PACKET', msg)

    @staticmethod
    def error(code, message):
        return "%s (%s)" % (code, message),

    @staticmethod
    def notifyNodeInformation(node_list):
        for node_type, address, uuid, state in node_list:
            address = '%s:%u' % address if address else '?'
            yield ' ! %s | %8s | %22s | %s' % (
                dump(uuid), node_type, address, state)

def main():
    global default_name
    db_path = sys.argv[1]
    default_name, _ = os.path.splitext(os.path.basename(db_path))
    db = sqlite3.connect(db_path)
    nl = db.execute('select * from log')
    np = db.execute('select * from packet')
    try:
        p = np.next()
    except StopIteration:
        p = None
    for date, name, level, pathname, lineno, msg in nl:
        try:
            while p and p[0] < date:
                packet(*p)
                p = np.next()
        except StopIteration:
            p = None
        emit(date, name, logging.getLevelName(level), msg.splitlines())
    if p:
        packet(*p)
        for p in np:
            packet(*p)


if __name__ == "__main__":
    sys.exit(main())
