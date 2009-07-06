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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import logging

from neo import protocol
from neo.storage.handlers.handler import BaseClientAndStorageOperationHandler
from neo.protocol import INVALID_SERIAL, INVALID_TID, INVALID_PARTITION, \
        TEMPORARILY_DOWN_STATE, DISCARDED_STATE, OUT_OF_DATE_STATE
from neo.util import dump


class TransactionInformation(object):
    """This class represents information on a transaction."""
    def __init__(self, uuid):
        self._uuid = uuid
        self._object_dict = {}
        self._transaction = None

    def getUUID(self):
        return self._uuid

    def addObject(self, oid, compression, checksum, data):
        self._object_dict[oid] = (oid, compression, checksum, data)

    def addTransaction(self, oid_list, user, desc, ext):
        self._transaction = (oid_list, user, desc, ext)

    def getObjectList(self):
        return self._object_dict.values()

    def getTransaction(self):
        return self._transaction


class ClientOperationHandler(BaseClientAndStorageOperationHandler):

    def dealWithClientFailure(self, uuid):
        app = self.app
        for tid, t in app.transaction_dict.items():
            if t.getUUID() == uuid:
                for o in t.getObjectList():
                    oid = o[0]
                    try:
                        del app.store_lock_dict[oid]
                        del app.load_lock_dict[oid]
                    except KeyError:
                        pass
                del app.transaction_dict[tid]
        # Now it may be possible to execute some events.
        app.executeQueuedEvents()

    def timeoutExpired(self, conn):
        self.dealWithClientFailure(conn.getUUID())
        BaseClientAndStorageOperationHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        self.dealWithClientFailure(conn.getUUID())
        BaseClientAndStorageOperationHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        self.dealWithClientFailure(conn.getUUID())
        BaseClientAndStorageOperationHandler.peerBroken(self, conn)

    def connectionCompleted(self, conn):
        BaseClientAndStorageOperationHandler.connectionCompleted(self, conn)

    def handleAskObject(self, conn, packet, oid, serial, tid):
        app = self.app
        if oid in app.load_lock_dict:
            # Delay the response.
            app.queueEvent(self.handleAskObject, conn, packet, oid,
                           serial, tid)
            return

        if serial == INVALID_SERIAL:
            serial = None
        if tid == INVALID_TID:
            tid = None
        o = app.dm.getObject(oid, serial, tid)
        if o is not None:
            serial, next_serial, compression, checksum, data = o
            if next_serial is None:
                next_serial = INVALID_SERIAL
            logging.debug('oid = %s, serial = %s, next_serial = %s',
                          dump(oid), dump(serial), dump(next_serial))
            p = protocol.answerObject(oid, serial, next_serial,
                           compression, checksum, data)
        else:
            logging.debug('oid = %s not found', dump(oid))
            p = protocol.oidNotFound('%s does not exist' % dump(oid))
        conn.answer(p, packet)

    def handleAbortTransaction(self, conn, packet, tid):
        uuid = conn.getUUID()
        app = self.app
        try:
            t = app.transaction_dict[tid]
            object_list = t.getObjectList()
            for o in object_list:
                oid = o[0]
                try:
                    del app.load_lock_dict[oid]
                except KeyError:
                    pass
                del app.store_lock_dict[oid]

            del app.transaction_dict[tid]

            # Now it may be possible to execute some events.
            app.executeQueuedEvents()
        except KeyError:
            pass

    def handleAskStoreTransaction(self, conn, packet, tid, user, desc,
                                  ext, oid_list):
        uuid = conn.getUUID()
        app = self.app
        t = app.transaction_dict.setdefault(tid, TransactionInformation(uuid))
        t.addTransaction(oid_list, user, desc, ext)
        conn.answer(protocol.answerStoreTransaction(tid), packet)

    def handleAskStoreObject(self, conn, packet, oid, serial,
                             compression, checksum, data, tid):
        uuid = conn.getUUID()
        # First, check for the locking state.
        app = self.app
        locking_tid = app.store_lock_dict.get(oid)
        if locking_tid is not None:
            if locking_tid < tid:
                # Delay the response.
                app.queueEvent(self.handleAskStoreObject, conn, packet,
                               oid, serial, compression, checksum,
                               data, tid)
            else:
                # If a newer transaction already locks this object,
                # do not try to resolve a conflict, so return immediately.
                logging.info('unresolvable conflict in %s', dump(oid))
                p = protocol.answerStoreObject(1, oid, locking_tid)
                conn.answer(p, packet)
            return

        # Next, check if this is generated from the latest revision.
        history_list = app.dm.getObjectHistory(oid)
        if history_list:
            last_serial = history_list[0][0]
            if last_serial != serial:
                logging.info('resolvable conflict in %s', dump(oid))
                p = protocol.answerStoreObject(1, oid, last_serial)
                conn.answer(p, packet)
                return
        # Now store the object.
        t = app.transaction_dict.setdefault(tid, TransactionInformation(uuid))
        t.addObject(oid, compression, checksum, data)
        p = protocol.answerStoreObject(0, oid, serial)
        conn.answer(p, packet)
        app.store_lock_dict[oid] = tid

