class DatabaseManager(object):
    """This class only describes an interface for database managers."""

    def __init__(self, **kwargs):
        """Initialize the object."""
        pass

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

    def getOIDListByTID(self, tid, all = False):
        """Return a list of the IDs of objects belonging to a given
        transaction. If such a transaction does not exist, return
        None rather than an empty list. If all is true, the data must
        be searched from unfinished transactions as well."""
        raise NotImplementedError('this method must be overridden')

    def objectPresent(self, oid, tid, all = True):
        """Return true iff an object specified by a given pair of an
        object ID and a transaction ID is present in a database.
        Otherwise, return false. If all is true, the object must be
        search from unfinished transactions as well."""
        raise NotImplementedError('this method must be overridden')

    def getObject(self, oid, tid = None, before_tid = None):
        """Return a tuple of an object ID, a serial, a compression
        specification, a checksum, and object data, if a given object
        ID is present. Otherwise, return None. If tid is None and
        before_tid is None, the latest revision is taken. If tid is
        specified, the given revision is taken. If tid is not specified,
        but before_tid is specified, the latest revision before the
        given revision is taken."""
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

    def storeTransaction(self, tid, object_list, transaction):
        """Store a transaction temporarily. Note that this transaction
        is not finished yet. The list of objects contains tuples,
        each of which consists of an object ID, a compression specification,
        a checksum and object data. The transaction is either None or
        a tuple of the list of oids, user information, a description and
        extension information."""
        raise NotImplementedError('this method must be overridden')

    def finishTransaction(self, tid):
        """Finish a transaction specified by a given ID, by moving
        temporarily data to a finished area."""
        raise NotImplementedError('this method must be overridden')
