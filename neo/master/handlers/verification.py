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

from neo import logging

from neo.master.handlers import BaseServiceHandler
from neo.exception import VerificationFailure
from neo.util import dump

class VerificationHandler(BaseServiceHandler):
    """This class deals with events for a verification phase."""

    def connectionCompleted(self, conn):
        pass

    def nodeLost(self, conn, node):
        if not self.app.pt.operational():
            raise VerificationFailure, 'cannot continue verification'

    def answerLastIDs(self, conn, packet, loid, ltid, lptid):
        app = self.app
        # If I get a bigger value here, it is dangerous.
        if app.loid < loid or app.ltid < ltid or app.pt.getID() < lptid:
            logging.critical('got later information in verification')
            raise VerificationFailure

    def answerUnfinishedTransactions(self, conn, packet, tid_list):
        uuid = conn.getUUID()
        logging.info('got unfinished transactions %s from %s:%d', 
                tid_list, *(conn.getAddress()))
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.unfinished_tid_set.update(tid_list)
        app.asking_uuid_dict[uuid] = True

    def answerTransactionInformation(self, conn, packet, tid,
                                           user, desc, ext, oid_list):
        uuid = conn.getUUID()
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

    def tidNotFound(self, conn, packet, message):
        uuid = conn.getUUID()
        logging.info('TID not found: %s', message)
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.unfinished_oid_set = None
        app.asking_uuid_dict[uuid] = True

    def answerObjectPresent(self, conn, packet, oid, tid):
        uuid = conn.getUUID()
        logging.info('object %s:%s found', dump(oid), dump(tid))
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.asking_uuid_dict[uuid] = True

    def oidNotFound(self, conn, packet, message):
        uuid = conn.getUUID()
        logging.info('OID not found: %s', message)
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.object_present = False
        app.asking_uuid_dict[uuid] = True
