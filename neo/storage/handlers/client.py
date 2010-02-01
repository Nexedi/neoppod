#
# Copyright (C) 2006-2010  Nexedi SA
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

from neo import logging

from neo import protocol
from neo.protocol import Packets
from neo.storage.handlers import BaseClientAndStorageOperationHandler
from neo.util import dump


class TransactionInformation(object):
    """This class represents information on a transaction."""
    def __init__(self, uuid):
        self._uuid = uuid
        self._object_dict = {}
        self._transaction = None
        self._last_oid_changed = False

    def lastOIDLchange(self):
        self._last_oid_changed = True

    def isLastOIDChanged(self):
        return self._last_oid_changed

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

    def abortTransaction(self, conn, tid):
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

    def askStoreTransaction(self, conn, tid, user, desc,
                                  ext, oid_list):
        uuid = conn.getUUID()
        app = self.app
        t = app.transaction_dict.setdefault(tid, TransactionInformation(uuid))
        if t.isLastOIDChanged():
            self.app.dm.setLastOID(self.app.loid)
        t.addTransaction(oid_list, user, desc, ext)
        conn.answer(Packets.AnswerStoreTransaction(tid))

    def askStoreObject(self, conn, oid, serial,
                             compression, checksum, data, tid):
        uuid = conn.getUUID()
        # First, check for the locking state.
        app = self.app
        locking_tid = app.store_lock_dict.get(oid)
        if locking_tid is not None:
            if locking_tid < tid:
                # Delay the response.
                app.queueEvent(self.askStoreObject, conn, oid, serial, 
                    compression, checksum, data, tid)
            else:
                # If a newer transaction already locks this object,
                # do not try to resolve a conflict, so return immediately.
                logging.info('unresolvable conflict in %s', dump(oid))
                conn.answer(Packets.AnswerStoreObject(1, oid, locking_tid))
            return

        # Next, check if this is generated from the latest revision.
        history_list = app.dm.getObjectHistory(oid)
        if history_list:
            last_serial = history_list[0][0]
            if last_serial != serial:
                logging.info('resolvable conflict in %s', dump(oid))
                conn.answer(Packets.AnswerStoreObject(1, oid, last_serial))
                return
        # Now store the object.
        t = app.transaction_dict.setdefault(tid, TransactionInformation(uuid))
        t.addObject(oid, compression, checksum, data)
        conn.answer(Packets.AnswerStoreObject(0, oid, serial))
        app.store_lock_dict[oid] = tid

        # check if a greater OID last the last generated was used
        if oid != protocol.INVALID_OID and oid > self.app.loid:
            args = dump(oid), dump(self.app.loid)
            logging.warning('Greater OID used in StoreObject : %s > %s', *args)
            self.app.loid = oid
            t.lastOIDLchange()

