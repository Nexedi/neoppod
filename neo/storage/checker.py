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

from collections import deque
from neo.lib import logging
from neo.lib.connection import ClientConnection, ConnectionClosed
from neo.lib.protocol import NodeTypes, Packets, ZERO_OID
from neo.lib.util import add64, dump
from .handlers.storage import StorageOperationHandler

CHECK_COUNT = 4000

class Checker(object):

    def __init__(self, app):
        self.app = app
        self.queue = deque()
        self.conn_dict = {}

    def __call__(self, partition, source, min_tid, max_tid):
        self.queue.append((partition, source, min_tid, max_tid))
        if not self.conn_dict:
            self._nextPartition()

    def _nextPartition(self):
        app = self.app
        def connect(node, uuid=app.uuid, name=app.name):
            if node.getUUID() == app.uuid:
                return
            if node.isConnected(connecting=True):
                conn = node.getConnection()
                conn.asClient()
            else:
                conn = ClientConnection(app, StorageOperationHandler(app), node)
                conn.ask(Packets.RequestIdentification(
                    NodeTypes.STORAGE, uuid, app.server, name))
            self.conn_dict[conn] = node.isIdentified()
        conn_set = set(self.conn_dict)
        conn_set.discard(None)
        try:
            self.conn_dict.clear()
            while True:
                try:
                    partition, (name, source), min_tid, max_tid = \
                        self.queue.popleft()
                except IndexError:
                    return
                cell = app.pt.getCell(partition, app.uuid)
                if cell is None or cell.isOutOfDate():
                    msg = "discarded or out-of-date"
                else:
                    try:
                        for cell in app.pt.getCellList(partition):
                            # XXX: Ignore corrupted cells for the moment
                            #      because we're still unable to fix them
                            #      (see also AdministrationHandler of master)
                            if cell.isReadable(): #if not cell.isOutOfDate():
                                connect(cell.getNode())
                        if source:
                            node = app.nm.getByAddress(source)
                            if name:
                                source = app.nm.createStorage(address=source) \
                                         if node is None else node
                                connect(source, None, name)
                            elif (node.getUUID() == app.uuid or
                                  node.isConnected(connecting=True) and
                                  node.getConnection() in self.conn_dict):
                                source = node
                            else:
                                msg = "unavailable source"
                        if self.conn_dict:
                            break
                        msg = "no replica"
                    except ConnectionClosed:
                        msg = "connection closed"
                    finally:
                        conn_set.update(self.conn_dict)
                    self.conn_dict.clear()
                logging.error("Failed to start checking partition %u (%s)",
                    partition, msg)
            conn_set.difference_update(self.conn_dict)
        finally:
            for conn in conn_set:
                app.closeClient(conn)
        logging.debug("start checking partition %u from %s to %s",
                      partition, dump(min_tid), dump(max_tid))
        self.min_tid = self.next_tid = min_tid
        self.max_tid = max_tid
        self.next_oid = None
        self.partition = partition
        self.source = source
        def start():
            if app.tm.isLockedTid(max_tid):
                app.queueEvent(start)
                return
            args = partition, CHECK_COUNT, min_tid, max_tid
            p = Packets.AskCheckTIDRange(*args)
            for conn, identified in self.conn_dict.items():
                self.conn_dict[conn] = conn.ask(p) if identified else None
            self.conn_dict[None] = app.dm.checkTIDRange(*args)
        start()

    def connected(self, node):
        conn = node.getConnection()
        if self.conn_dict.get(conn, self) is None:
            self.conn_dict[conn] = conn.ask(Packets.AskCheckTIDRange(
                self.partition, CHECK_COUNT, self.next_tid, self.max_tid))

    def connectionLost(self, conn):
        try:
            del self.conn_dict[conn]
        except KeyError:
            return
        if self.source is not None and self.source.getConnection() is conn:
            del self.source
        elif len(self.conn_dict) > 1:
            logging.warning("node lost but keep up checking partition %u",
                            self.partition)
            return
        logging.warning("check of partition %u aborted", self.partition)
        self._nextPartition()

    def _nextRange(self):
        if self.next_oid:
            args = self.partition, CHECK_COUNT, self.next_tid, self.max_tid, \
                self.next_oid
            p = Packets.AskCheckSerialRange(*args)
            check = self.app.dm.checkSerialRange
        else:
            args = self.partition, CHECK_COUNT, self.next_tid, self.max_tid
            p = Packets.AskCheckTIDRange(*args)
            check = self.app.dm.checkTIDRange
        for conn in self.conn_dict.keys():
            self.conn_dict[conn] = check(*args) if conn is None else conn.ask(p)

    def checkRange(self, conn, *args):
        if self.conn_dict.get(conn, self) != conn.getPeerId():
            # Ignore answers to old requests,
            # because we did nothing to cancel them.
            logging.info("ignored AnswerCheck*Range%r", args)
            return
        self.conn_dict[conn] = args
        answer_set = set(self.conn_dict.itervalues())
        if len(answer_set) > 1:
            for answer in answer_set:
                if type(answer) is not tuple:
                    return
            # TODO: Automatically tell corrupted cells to fix their data
            #       if we know a good source.
            #       For the moment, tell master to put them in CORRUPTED state
            #       and keep up checking if useful.
            uuid = self.app.uuid
            args = None if self.source is None else self.conn_dict[
                None if self.source.getUUID() == uuid
                     else self.source.getConnection()]
            uuid_list = []
            for conn, answer in self.conn_dict.items():
                if answer != args:
                    del self.conn_dict[conn]
                    if conn is None:
                        uuid_list.append(uuid)
                    else:
                        uuid_list.append(conn.getUUID())
                        self.app.closeClient(conn)
            p = Packets.NotifyPartitionCorrupted(self.partition, uuid_list)
            self.app.master_conn.notify(p)
            if len(self.conn_dict) <= 1:
                logging.warning("check of partition %u aborted", self.partition)
                self.queue.clear()
                self._nextPartition()
                return
        try:
            count, _, max_tid = args
        except ValueError:
            count, _, self.next_tid, _, max_oid = args
            if count < CHECK_COUNT:
                logging.debug("partition %u checked from %s to %s",
                    self.partition, dump(self.min_tid), dump(self.max_tid))
                self._nextPartition()
                return
            self.next_oid = add64(max_oid, 1)
        else:
            (count, _, max_tid), = answer_set
            if count < CHECK_COUNT:
                self.next_tid = self.min_tid
                self.next_oid = ZERO_OID
            else:
                self.next_tid = add64(max_tid, 1)
        self._nextRange()
