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

class DatabaseManager(object):
    """This class only describes an interface for database managers."""

    def __init__(self, **kwargs):
        """Initialize the object."""
        pass

    def close(self):
        """ close the database connection """
        raise NotImplementedError('this method must be overridden')

    def setup(self, reset = 0):
        """Set up a database. If reset is true, existing data must be
        discarded."""
        raise NotImplementedError('this method must be overridden')

    def getUUID(self):
        """Load an UUID from a database. If not present, return None."""
        raise NotImplementedError('this method must be overridden')

    def setUUID(self, uuid):
        """Store an UUID into a database."""
        raise NotImplementedError('this method must be overridden')

    def getNumPartitions(self):
        """Load the number of partitions from a database. If not present,
        return None."""
        raise NotImplementedError('this method must be overridden')

    def setNumPartitions(self, num_partitions):
        """Store the number of partitions into a database."""
        raise NotImplementedError('this method must be overridden')

    def getNumReplicas(self):
        """Load the number of replicas from a database. If not present,
        return None."""
        raise NotImplementedError('this method must be overridden')

    def setNumReplicas(self, num_partitions):
        """Store the number of replicas into a database."""
        raise NotImplementedError('this method must be overridden')

    def getName(self):
        """Load a name from a database. If not present, return None."""
        raise NotImplementedError('this method must be overridden')

    def setName(self, name):
        """Store a name into a database."""
        raise NotImplementedError('this method must be overridden')

    def getPTID(self):
        """Load a Partition Table ID from a database. If not present,
        return None."""
        raise NotImplementedError('this method must be overridden')

    def setPTID(self, ptid):
        """Store a Partition Table ID into a database."""
        raise NotImplementedError('this method must be overridden')

    def getPartitionTable(self):
        """Return a whole partition table as a tuple of rows. Each row
        is again a tuple of an offset (row ID), an UUID of a storage
        node, and a cell state."""
        raise NotImplementedError('this method must be overridden')

    def getLastOID(self, all = True):
        """Return the last OID in a database. If all is true,
        unfinished transactions must be taken account into. If there
        is no OID in the database, return None."""
        raise NotImplementedError('this method must be overridden')

    def getLastTID(self, all = True):
        """Return the last TID in a database. If all is true,
        unfinished transactions must be taken account into. If there
        is no TID in the database, return None."""
        raise NotImplementedError('this method must be overridden')

    def getUnfinishedTIDList(self):
        """Return a list of unfinished transaction's IDs."""
        raise NotImplementedError('this method must be overridden')

    def objectPresent(self, oid, tid, all = True):
        """Return true iff an object specified by a given pair of an
        object ID and a transaction ID is present in a database.
        Otherwise, return false. If all is true, the object must be
        searched from unfinished transactions as well."""
        raise NotImplementedError('this method must be overridden')

    def getObject(self, oid, tid = None, before_tid = None):
        """Return a tuple of a serial, next serial, a compression
        specification, a checksum, and object data, if a given object
        ID is present. Otherwise, return None. If tid is None and
        before_tid is None, the latest revision is taken. If tid is
        specified, the given revision is taken. If tid is not specified,
        but before_tid is specified, the latest revision before the
        given revision is taken. The next serial is a serial right after
        before_tid, if specified. Otherwise, it is None."""
        raise NotImplementedError('this method must be overridden')

    def changePartitionTable(self, ptid, cell_list):
        """Change a part of a partition table. The list of cells is
        a tuple of tuples, each of which consists of an offset (row ID),
        an UUID of a storage node, and a cell state. The Partition
        Table ID must be stored as well."""
        raise NotImplementedError('this method must be overridden')

    def setPartitionTable(self, ptid, cell_list):
        """Set a whole partition table. The semantics is the same as
        changePartitionTable, except that existing data must be
        thrown away."""
        raise NotImplementedError('this method must be overridden')

    def dropPartition(self, offset):
        """ Drop objects and transactions assigned to a partition table,
        this should be called only during storage initialization, to clear
        existing data that could be stored by a previous cluster life """
        raise NotImplementedError('this method must be overriden')

    def dropUnfinishedData(self):
        """Drop any unfinished data from a database."""
        raise NotImplementedError('this method must be overridden')

    def storeTransaction(self, tid, object_list, transaction, temporary = True):
        """Store a transaction temporarily, if temporary is true. Note
        that this transaction is not finished yet. The list of objects
        contains tuples, each of which consists of an object ID,
        a compression specification, a checksum and object data.
        The transaction is either None or a tuple of the list of OIDs,
        user information, a description and extension information."""
        raise NotImplementedError('this method must be overridden')

    def finishTransaction(self, tid):
        """Finish a transaction specified by a given ID, by moving
        temporarily data to a finished area."""
        raise NotImplementedError('this method must be overridden')

    def deleteTransaction(self, tid, all = False):
        """Delete a transaction specified by a given ID from a temporarily
        area. If all is true, it must be deleted even from a finished
        area."""
        raise NotImplementedError('this method must be overridden')

    def getTransaction(self, tid, all = False):
        """Return a tuple of the list of OIDs, user information,
        a description, and extension information, for a given transaction
        ID. If there is no such transaction ID in a database, return None.
        If all is true, the transaction must be searched from a temporary
        area as well."""
        raise NotImplementedError('this method must be overridden')

    def getOIDList(self, offset, length, num_partitions, partition_list):
        """Return a list of OIDs in descending order from an offset,
        at most the specified length. The list of partitions are passed
        to filter out non-applicable TIDs."""
        raise NotImplementedError('this method must be overridden')

    def getObjectHistory(self, oid, offset = 0, length = 1):
        """Return a list of serials and sizes for a given object ID.
        The length specifies the maximum size of such a list. The first serial
        must be the last serial, and the list must be sorted in descending
        order. If there is no such object ID in a database, return None."""
        raise NotImplementedError('this method must be overridden')

    def getTIDList(self, offset, length, num_partitions, partition_list):
        """Return a list of TIDs in descending order from an offset,
        at most the specified length. The list of partitions are passed
        to filter out non-applicable TIDs."""
        raise NotImplementedError('this method must be overridden')

    def getTIDListPresent(self, tid_list):
        """Return a list of TIDs which are present in a database among
        the given list."""
        raise NotImplementedError('this method must be overridden')

    def getSerialListPresent(self, oid, serial_list):
        """Return a list of serials which are present in a database among
        the given list."""
        raise NotImplementedError('this method must be overridden')

