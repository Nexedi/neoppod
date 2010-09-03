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

from neo import util
from neo.exception import DatabaseFailure

class DatabaseManager(object):
    """This class only describes an interface for database managers."""

    def __init__(self):
        """
            Initialize the object.
        """
        self._under_transaction = False

    def isUnderTransaction(self):
        return self._under_transaction

    def begin(self):
        """
            Begin a transaction
        """
        if self._under_transaction:
            raise DatabaseFailure('A transaction has already begun')
        self._begin()
        self._under_transaction = True

    def commit(self):
        """
            Commit the current transaction
        """
        if not self._under_transaction:
            raise DatabaseFailure('The transaction has not begun')
        self._commit()
        self._under_transaction = False

    def rollback(self):
        """
            Rollback the current transaction
        """
        self._rollback()
        self._under_transaction = False

    def setup(self, reset = 0):
        """Set up a database. If reset is true, existing data must be
        discarded."""
        raise NotImplementedError

    def _begin(self):
        raise NotImplementedError

    def _commit(self):
        raise NotImplementedError

    def _rollback(self):
        raise NotImplementedError

    def getConfiguration(self, key):
        """
            Return a configuration value, returns None if not found or not set
        """
        raise NotImplementedError

    def setConfiguration(self, key, value):
        """
            Set a configuration value
        """
        if self._under_transaction:
            self._setConfiguration(key, value)
        else:
            self.begin()
            try:
                self._setConfiguration(key, value)
            except:
                self.rollback()
                raise
            self.commit()

    def _setConfiguration(self, key, value):
        raise NotImplementedError

    def getUUID(self):
        """
            Load an UUID from a database.
        """
        return util.bin(self.getConfiguration('uuid'))

    def setUUID(self, uuid):
        """
            Store an UUID into a database.
        """
        self.setConfiguration('uuid', util.dump(uuid))

    def getNumPartitions(self):
        """
            Load the number of partitions from a database.
        """
        n = self.getConfiguration('partitions')
        if n is not None:
            return int(n)

    def setNumPartitions(self, num_partitions):
        """
            Store the number of partitions into a database.
        """
        self.setConfiguration('partitions', num_partitions)

    def getNumReplicas(self):
        """
            Load the number of replicas from a database.
        """
        n = self.getConfiguration('replicas')
        if n is not None:
            return int(n)

    def setNumReplicas(self, num_replicas):
        """
            Store the number of replicas into a database.
        """
        self.setConfiguration('replicas', num_replicas)

    def getName(self):
        """
            Load a name from a database.
        """
        return self.getConfiguration('name')

    def setName(self, name):
        """
            Store a name into a database.
        """
        self.setConfiguration('name', name)

    def getPTID(self):
        """
            Load a Partition Table ID from a database.
        """
        return util.bin(self.getConfiguration('ptid'))

    def setPTID(self, ptid):
        """
            Store a Partition Table ID into a database.
        """
        self.setConfiguration('ptid', util.dump(ptid))

    def getLastOID(self):
        """
            Returns the last OID used
        """
        return util.bin(self.getConfiguration('loid'))

    def setLastOID(self, loid):
        """
            Set the last OID used
        """
        self.setConfiguration('loid', util.dump(loid))

    def getPartitionTable(self):
        """Return a whole partition table as a tuple of rows. Each row
        is again a tuple of an offset (row ID), an UUID of a storage
        node, and a cell state."""
        raise NotImplementedError

    def getLastTID(self, all = True):
        """Return the last TID in a database. If all is true,
        unfinished transactions must be taken account into. If there
        is no TID in the database, return None."""
        raise NotImplementedError

    def getUnfinishedTIDList(self):
        """Return a list of unfinished transaction's IDs."""
        raise NotImplementedError

    def objectPresent(self, oid, tid, all = True):
        """Return true iff an object specified by a given pair of an
        object ID and a transaction ID is present in a database.
        Otherwise, return false. If all is true, the object must be
        searched from unfinished transactions as well."""
        raise NotImplementedError

    def getObject(self, oid, tid = None, before_tid = None):
        """Return a tuple of a serial, next serial, a compression
        specification, a checksum, and object data, if a given object
        ID is present. Otherwise, return None. If tid is None and
        before_tid is None, the latest revision is taken. If tid is
        specified, the given revision is taken. If tid is not specified,
        but before_tid is specified, the latest revision before the
        given revision is taken. The next serial is a serial right after
        before_tid, if specified. Otherwise, it is None."""
        raise NotImplementedError

    def changePartitionTable(self, ptid, cell_list):
        """Change a part of a partition table. The list of cells is
        a tuple of tuples, each of which consists of an offset (row ID),
        an UUID of a storage node, and a cell state. The Partition
        Table ID must be stored as well."""
        raise NotImplementedError

    def setPartitionTable(self, ptid, cell_list):
        """Set a whole partition table. The semantics is the same as
        changePartitionTable, except that existing data must be
        thrown away."""
        raise NotImplementedError

    def dropPartitions(self, num_partitions, offset_list):
        """ Drop any data of non-assigned partitions for a given UUID """
        raise NotImplementedError('this method must be overriden')

    def dropUnfinishedData(self):
        """Drop any unfinished data from a database."""
        raise NotImplementedError

    def storeTransaction(self, tid, object_list, transaction, temporary = True):
        """Store a transaction temporarily, if temporary is true. Note
        that this transaction is not finished yet. The list of objects
        contains tuples, each of which consists of an object ID,
        a compression specification, a checksum and object data.
        The transaction is either None or a tuple of the list of OIDs,
        user information, a description, extension information and transaction
        pack state (True for packed)."""
        raise NotImplementedError

    def findUndoTID(self, oid, tid, undone_tid, transaction_object):
        """
        oid
            Object OID
        tid
            Transation doing the undo
        undone_tid
            Transaction to undo
        transaction_object
            Object data from memory, if it was modified by running
            transaction.
            None if is was not modified by running transaction.

        Returns a 3-tuple:
        current_tid (p64)
            TID of most recent version of the object client's transaction can
            see. This is used later to detect current conflicts (eg, another
            client modifying the same object in parallel)
        data_tid (int)
            TID containing (without indirection) the data prior to undone
            transaction.
            None if object doesn't exist prior to transaction being undone
              (its creation is being undone).
        is_current (bool)
            False if object was modified by later transaction (ie, data_tid is
            not current), True otherwise.
        """
        raise NotImplementedError

    def finishTransaction(self, tid):
        """Finish a transaction specified by a given ID, by moving
        temporarily data to a finished area."""
        raise NotImplementedError

    def deleteTransaction(self, tid, all = False):
        """Delete a transaction specified by a given ID from a temporarily
        area. If all is true, it must be deleted even from a finished
        area."""
        raise NotImplementedError

    def getTransaction(self, tid, all = False):
        """Return a tuple of the list of OIDs, user information,
        a description, and extension information, for a given transaction
        ID. If there is no such transaction ID in a database, return None.
        If all is true, the transaction must be searched from a temporary
        area as well."""
        raise NotImplementedError

    def getOIDList(self, min_oid, length, num_partitions, partition_list):
        """Return a list of OIDs in ascending order from a minimal oid,
        at most the specified length. The list of partitions are passed
        to filter out non-applicable TIDs."""
        raise NotImplementedError

    def getObjectHistory(self, oid, offset = 0, length = 1):
        """Return a list of serials and sizes for a given object ID.
        The length specifies the maximum size of such a list. Result starts
        with latest serial, and the list must be sorted in descending order.
        If there is no such object ID in a database, return None."""
        raise NotImplementedError

    def getObjectHistoryFrom(self, oid, min_serial, length):
        """Return a list of length serials for a given object ID at (or above)
        min_serial, sorted in ascending order."""
        raise NotImplementedError

    def getTIDList(self, offset, length, num_partitions, partition_list):
        """Return a list of TIDs in ascending order from an offset,
        at most the specified length. The list of partitions are passed
        to filter out non-applicable TIDs."""
        raise NotImplementedError

    def getReplicationTIDList(self, min_tid, length, num_partitions,
        partition_list):
        """Return a list of TIDs in ascending order from an initial tid value,
        at most the specified length. The list of partitions are passed
        to filter out non-applicable TIDs."""
        raise NotImplementedError

    def getTIDListPresent(self, tid_list):
        """Return a list of TIDs which are present in a database among
        the given list."""
        raise NotImplementedError

    def getSerialListPresent(self, oid, serial_list):
        """Return a list of serials which are present in a database among
        the given list."""
        raise NotImplementedError

    def pack(self, tid, updateObjectDataForPack):
        """Prune all non-current object revisions at given tid.
        updateObjectDataForPack is a function called for each deleted object
        and revision with:
        - OID
        - packed TID
        - new value_serial
            If object data was moved to an after-pack-tid revision, this
            parameter contains the TID of that revision, allowing to backlink
            to it.
        - getObjectData function
            To call if value_serial is None and an object needs to be updated.
            Takes no parameter, returns a 3-tuple: compression, checksum,
            value
        """

