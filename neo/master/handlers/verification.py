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

from neo.protocol import CLIENT_NODE_TYPE, RUNNING_STATE, BROKEN_STATE, \
        TEMPORARILY_DOWN_STATE, ADMIN_NODE_TYPE
from neo.master.handlers import MasterHandler
from neo.exception import VerificationFailure
from neo.util import dump

class VerificationHandler(MasterHandler):
    """This class deals with events for a verification phase."""

    def connectionCompleted(self, conn):
        pass

    def _nodeLost(self, conn, node):
        if not self.app.pt.operational():
            raise VerificationFailure, 'cannot continue verification'

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        for node_type, addr, uuid, state in node_list:
            if node_type in (CLIENT_NODE_TYPE, ADMIN_NODE_TYPE):
                # No interest.
                continue
            
            if uuid is None:
                # No interest.
                continue

            if app.uuid == uuid:
                # This looks like me...
                if state == RUNNING_STATE:
                    # Yes, I know it.
                    continue
                else:
                    # What?! What happened to me?
                    raise RuntimeError, 'I was told that I am bad'

            node = app.nm.getNodeByUUID(uuid)
            if node is None:
                node = app.nm.getNodeByServer(addr)
                if node is None:
                    # I really don't know such a node. What is this?
                    continue
            else:
                if node.getServer() != addr:
                    # This is different from what I know.
                    continue

            if node.getState() == state:
                # No change. Don't care.
                continue

            if state == RUNNING_STATE:
                # No problem.
                continue

            # Something wrong happened possibly. Cut the connection to this node,
            # if any, and notify the information to others.
            # XXX this can be very slow.
            c = app.em.getConnectionByUUID(uuid)
            if c is not None:
                c.close()
            node.setState(state)
            app.broadcastNodeInformation(node)

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        app = self.app
        # If I get a bigger value here, it is dangerous.
        if app.loid < loid or app.ltid < ltid or app.lptid < lptid:
            logging.critical('got later information in verification')
            raise VerificationFailure

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        uuid = conn.getUUID()
        logging.info('got unfinished transactions %s from %s:%d', 
                tid_list, *(conn.getAddress()))
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.unfinished_tid_set.update(tid_list)
        app.asking_uuid_dict[uuid] = True

    def handleAnswerTransactionInformation(self, conn, packet, tid,
                                           user, desc, ext, oid_list):
        uuid = conn.getUUID()
        logging.info('got OIDs %s for %s from %s:%d', 
                oid_list, tid, *(conn.getAddress()))
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        oid_set = set(oid_list)
        if app.unfinished_oid_set is None:
            # Someone does not agree.
            pass
        elif len(app.unfinished_oid_set) == 0:
            # This is the first answer.
            app.unfinished_oid_set.update(oid_set)
        elif app.unfinished_oid_set != oid_set:
            app.unfinished_oid_set = None
        app.asking_uuid_dict[uuid] = True

    def handleTidNotFound(self, conn, packet, message):
        uuid = conn.getUUID()
        logging.info('TID not found: %s', message)
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.unfinished_oid_set = None
        app.asking_uuid_dict[uuid] = True

    def handleAnswerObjectPresent(self, conn, packet, oid, tid):
        uuid = conn.getUUID()
        logging.info('object %s:%s found', dump(oid), dump(tid))
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.asking_uuid_dict[uuid] = True

    def handleOidNotFound(self, conn, packet, message):
        uuid = conn.getUUID()
        logging.info('OID not found: %s', message)
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.object_present = False
        app.asking_uuid_dict[uuid] = True
