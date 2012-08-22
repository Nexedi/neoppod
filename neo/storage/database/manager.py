#
# Copyright (C) 2006-2012  Nexedi SA
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

from neo.lib import logging, util
from neo.lib.protocol import ZERO_TID

class CreationUndone(Exception):
    pass

class DatabaseManager(object):
    """This class only describes an interface for database managers."""

    def __init__(self, database, wait=0):
        """
            Initialize the object.
        """
        self._wait = wait
        self._parse(database)

    def _parse(self, database):
        """Called during instanciation, to process database parameter."""
        pass

    def setup(self, reset = 0):
        """Set up a database

        It must recover self._uncommitted_data from temporary object table.
        _uncommitted_data is a dict containing refcounts to data of
        write-locked objects, except in case of undo, where the refcount is
        increased later, when the object is read-locked.
        Keys are data ids and values are number of references.

        If reset is true, existing data must be discarded and
        self._uncommitted_data must be an empty dict.
        """
        raise NotImplementedError

    def commit(self):
        pass

    def _getPartition(self, oid_or_tid):
        return oid_or_tid % self.getNumPartitions()

    def getConfiguration(self, key):
        """
            Return a configuration value, returns None if not found or not set
        """
        raise NotImplementedError

    def setConfiguration(self, key, value):
        """
            Set a configuration value
        """
        self._setConfiguration(key, value)
        self.commit()

    def _setConfiguration(self, key, value):
        raise NotImplementedError

    def getUUID(self):
        """
            Load a NID from a database.
        """
        nid = self.getConfiguration('nid')
        if nid is not None:
            return int(nid)

    def setUUID(self, nid):
        """
            Store a NID into a database.
        """
        self.setConfiguration('nid', str(nid))

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
        ptid = self.getConfiguration('ptid')
        if ptid is not None:
            return long(ptid)

    def setPTID(self, ptid):
        """
            Store a Partition Table ID into a database.
        """
        if ptid is not None:
            assert isinstance(ptid, (int, long)), ptid
            ptid = str(ptid)
        self.setConfiguration('ptid', ptid)

    def getBackupTID(self):
        return util.bin(self.getConfiguration('backup_tid'))

    def setBackupTID(self, backup_tid):
        tid = util.dump(backup_tid)
        logging.debug('backup_tid = %s', tid)
        return self.setConfiguration('backup_tid', tid)

    def getPartitionTable(self):
        """Return a whole partition table as a sequence of rows. Each row
        is again a tuple of an offset (row ID), the NID of a storage
        node, and a cell state."""
        raise NotImplementedError

    def _getLastIDs(self, all=True):
        raise NotImplementedError

    def getLastIDs(self, all=True):
        trans, obj, oid = self._getLastIDs()
        if trans:
            tid = max(trans.itervalues())
            if obj:
                tid = max(tid, max(obj.itervalues()))
        else:
            tid = max(obj.itervalues()) if obj else None
        return tid, trans, obj, oid

    def getUnfinishedTIDList(self):
        """Return a list of unfinished transaction's IDs."""
        raise NotImplementedError

    def objectPresent(self, oid, tid, all = True):
        """Return true iff an object specified by a given pair of an
        object ID and a transaction ID is present in a database.
        Otherwise, return false. If all is true, the object must be
        searched from unfinished transactions as well."""
        raise NotImplementedError

    def _getObject(self, oid, tid=None, before_tid=None):
        """
        oid (int)
            Identifier of object to retrieve.
        tid (int, None)
            Exact serial to retrieve.
        before_tid (packed, None)
            Serial to retrieve is the highest existing one strictly below this
            value.
        """
        raise NotImplementedError

    def getObject(self, oid, tid=None, before_tid=None):
        """
        oid (packed)
            Identifier of object to retrieve.
        tid (packed, None)
            Exact serial to retrieve.
        before_tid (packed, None)
            Serial to retrieve is the highest existing one strictly below this
            value.

        Return value:
            None: Given oid doesn't exist in database.
            False: No record found, but another one exists for given oid.
            6-tuple: Record content.
                - record serial (packed)
                - serial or next record modifying object (packed, None)
                - compression (boolean-ish, None)
                - checksum (integer, None)
                - data (binary string, None)
                - data_serial (packed, None)
        """
        u64 = util.u64
        p64 = util.p64
        oid = u64(oid)
        if tid is not None:
            tid = u64(tid)
        if before_tid is not None:
            before_tid = u64(before_tid)
        result = self._getObject(oid, tid, before_tid)
        if result:
            serial, next_serial, compression, checksum, data, data_serial = \
                result
            assert before_tid is None or next_serial is None or \
                   before_tid <= next_serial
            if serial is not None:
                serial = p64(serial)
            if next_serial is not None:
                next_serial = p64(next_serial)
            if data_serial is not None:
                data_serial = p64(data_serial)
            return serial, next_serial, compression, checksum, data, data_serial
        # See if object exists at all
        return self._getObject(oid) and False

    def changePartitionTable(self, ptid, cell_list):
        """Change a part of a partition table. The list of cells is
        a tuple of tuples, each of which consists of an offset (row ID),
        the NID of a storage node, and a cell state. The Partition
        Table ID must be stored as well."""
        raise NotImplementedError

    def setPartitionTable(self, ptid, cell_list):
        """Set a whole partition table. The semantics is the same as
        changePartitionTable, except that existing data must be
        thrown away."""
        raise NotImplementedError

    def dropPartitions(self, offset_list):
        """Delete all data for specified partitions"""
        raise NotImplementedError

    def dropUnfinishedData(self):
        """Drop any unfinished data from a database."""
        raise NotImplementedError

    def storeTransaction(self, tid, object_list, transaction, temporary = True):
        """Store a transaction temporarily, if temporary is true. Note
        that this transaction is not finished yet. The list of objects
        contains tuples, each of which consists of an object ID,
        a data_id and object serial.
        The transaction is either None or a tuple of the list of OIDs,
        user information, a description, extension information and transaction
        pack state (True for packed)."""
        raise NotImplementedError

    def _pruneData(self, data_id_list):
        """To be overriden by the backend to delete any unreferenced data

        'unreferenced' means:
        - not in self._uncommitted_data
        - and not referenced by a fully-committed object (storage should have
          an index or a refcount of all data ids of all objects)
        """
        raise NotImplementedError

    def _storeData(self, checksum, data, compression):
        """To be overriden by the backend to store object raw data

        If same data was already stored, the storage only has to check there's
        no hash collision.
        """
        raise NotImplementedError

    def storeData(self, checksum_or_id, data=None, compression=None):
        """Store object raw data

        checksum must be the result of neo.lib.util.makeChecksum(data)
        'compression' indicates if 'data' is compressed.
        A volatile reference is set to this data until 'unlockData' is called
        with this checksum.
        If called with only an id, it only increment the volatile
        reference to the data matching the id.
        """
        refcount = self._uncommitted_data
        if data is not None:
            checksum_or_id = self._storeData(checksum_or_id, data, compression)
        refcount[checksum_or_id] = 1 + refcount.get(checksum_or_id, 0)
        return checksum_or_id

    def unlockData(self, data_id_list, prune=False):
        """Release 1 volatile reference to given list of checksums

        If 'prune' is true, any data that is not referenced anymore (either by
        a volatile reference or by a fully-committed object) is deleted.
        """
        refcount = self._uncommitted_data
        for data_id in data_id_list:
            count = refcount[data_id] - 1
            if count:
                refcount[data_id] = count
            else:
                del refcount[data_id]
        if prune:
            self._pruneData(data_id_list)
            self.commit()

    __getDataTID = set()
    def _getDataTID(self, oid, tid=None, before_tid=None):
        """
        Return a 2-tuple:
        tid (int)
            tid corresponding to received parameters
        serial
            tid at which actual object data is located

        If 'tid is None', requested object and transaction could
        not be found.
        If 'serial is None', requested object exist but has no data (its creation
        has been undone).
        If 'tid == serial', it means that requested transaction
        contains object data.
        Otherwise, it's an undo transaction which did not involve conflict
        resolution.
        """
        if self.__class__ not in self.__getDataTID:
            self.__getDataTID.add(self.__class__)
            logging.warning("Fallback to generic/slow implementation"
                " of _getDataTID. It should be overriden by backend storage.")
        r = self._getObject(oid, tid, before_tid)
        if r:
            serial, _, _, data_id, _, value_serial = r
            if value_serial is None and data_id:
                return serial, serial
            return serial, value_serial
        return None, None

    def findUndoTID(self, oid, tid, ltid, undone_tid, transaction_object):
        """
        oid
            Object OID
        tid
            Transation doing the undo
        ltid
            Upper (exclued) bound of transactions visible to transaction doing
            the undo.
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
        u64 = util.u64
        p64 = util.p64
        oid = u64(oid)
        tid = u64(tid)
        if ltid:
            ltid = u64(ltid)
        undone_tid = u64(undone_tid)
        def getDataTID(tid=None, before_tid=None):
            tid, value_serial = self._getDataTID(oid, tid, before_tid)
            if value_serial not in (None, tid):
                if value_serial >= tid:
                    raise ValueError("Incorrect value reference found for"
                                     " oid %d at tid %d: reference = %d"
                                     % (oid, value_serial, tid))
                if value_serial != getDataTID(value_serial)[1]:
                    logging.warning("Multiple levels of indirection"
                        " when getting data serial for oid %d at tid %d."
                        " This causes suboptimal performance.", oid, tid)
            return tid, value_serial
        if transaction_object:
            current_tid = current_data_tid = u64(transaction_object[2])
        else:
            current_tid, current_data_tid = getDataTID(before_tid=ltid)
        if current_tid is None:
            return (None, None, False)
        found_undone_tid, undone_data_tid = getDataTID(tid=undone_tid)
        assert found_undone_tid is not None, (oid, undone_tid)
        is_current = undone_data_tid in (current_data_tid, tid)
        # Load object data as it was before given transaction.
        # It can be None, in which case it means we are undoing object
        # creation.
        _, data_tid = getDataTID(before_tid=undone_tid)
        if data_tid is not None:
            data_tid = p64(data_tid)
        return p64(current_tid), data_tid, is_current

    def finishTransaction(self, tid):
        """Finish a transaction specified by a given ID, by moving
        temporarily data to a finished area."""
        raise NotImplementedError

    def deleteTransaction(self, tid, oid_list=()):
        """Delete a transaction and its content specified by a given ID and
        an oid list"""
        raise NotImplementedError

    def deleteObject(self, oid, serial=None):
        """Delete given object. If serial is given, only delete that serial for
        given oid."""
        raise NotImplementedError

    def _deleteRange(self, partition, min_tid=None, max_tid=None):
        """Delete all objects and transactions between given min_tid (excluded)
        and max_tid (included)"""
        raise NotImplementedError

    def truncate(self, tid):
        assert tid not in (None, ZERO_TID), tid
        assert self.getBackupTID()
        self.setBackupTID(None) # XXX
        for partition in xrange(self.getNumPartitions()):
            self._deleteRange(partition, tid)
        self.commit()

    def getTransaction(self, tid, all = False):
        """Return a tuple of the list of OIDs, user information,
        a description, and extension information, for a given transaction
        ID. If there is no such transaction ID in a database, return None.
        If all is true, the transaction must be searched from a temporary
        area as well."""
        raise NotImplementedError

    def getObjectHistory(self, oid, offset = 0, length = 1):
        """Return a list of serials and sizes for a given object ID.
        The length specifies the maximum size of such a list. Result starts
        with latest serial, and the list must be sorted in descending order.
        If there is no such object ID in a database, return None."""
        raise NotImplementedError

    def getReplicationObjectList(self, min_tid, max_tid, length, partition,
            min_oid):
        """Return a dict of length oids grouped by serial at (or above)
        min_tid and min_oid and below max_tid, for given partition,
        sorted in ascending order."""
        raise NotImplementedError

    def getTIDList(self, offset, length, partition_list):
        """Return a list of TIDs in ascending order from an offset,
        at most the specified length. The list of partitions are passed
        to filter out non-applicable TIDs."""
        raise NotImplementedError

    def getReplicationTIDList(self, min_tid, max_tid, length, partition):
        """Return a list of TIDs in ascending order from an initial tid value,
        at most the specified length up to max_tid. The partition number is
        passed to filter out non-applicable TIDs."""
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
            Takes no parameter, returns a 3-tuple: compression, data_id,
            value
        """
        raise NotImplementedError

    def checkTIDRange(self, partition, length, min_tid, max_tid):
        """
        Generate a diggest from transaction list.
        min_tid (packed)
            TID at which verification starts.
        length (int)
            Maximum number of records to include in result.

        Returns a 3-tuple:
            - number of records actually found
            - a SHA1 computed from record's TID
              ZERO_HASH if no record found
            - biggest TID found (ie, TID of last record read)
              ZERO_TID if not record found
        """
        raise NotImplementedError

    def checkSerialRange(self, partition, length, min_tid, max_tid, min_oid):
        """
        Generate a diggest from object list.
        min_oid (packed)
            OID at which verification starts.
        min_tid (packed)
            Serial of min_oid object at which search should start.
        length
            Maximum number of records to include in result.

        Returns a 5-tuple:
            - number of records actually found
            - a SHA1 computed from record's OID
              ZERO_HASH if no record found
            - biggest OID found (ie, OID of last record read)
              ZERO_OID if no record found
            - a SHA1 computed from record's serial
              ZERO_HASH if no record found
            - biggest serial found for biggest OID found (ie, serial of last
              record read)
              ZERO_TID if no record found
        """
        raise NotImplementedError

