#
# Copyright (C) 2010  Nexedi SA
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
"""
Naive b-tree implementation.
Simple, though not so well tested.
Not persistent ! (no data retained after process exit)
"""

from BTrees.OOBTree import OOBTree as _OOBTree
import neo.lib
from hashlib import md5

from neo.storage.database import DatabaseManager
from neo.lib.protocol import CellStates, ZERO_OID, ZERO_TID
from neo.lib import util

# The only purpose of this value (and code using it) is to avoid creating
# arbitrarily-long lists of values when cleaning up dictionaries.
KEY_BATCH_SIZE = 1000

# Keep dropped trees in memory to avoid instanciating when not needed.
TREE_POOL = []
# How many empty BTree istance to keep in ram
MAX_TREE_POOL_SIZE = 100

def batchDelete(tree, tester_callback, iter_kw=None, recycle_subtrees=False):
    """
    Iter over given BTree and delete found entries.
    tree BTree
        Tree to delete entries from.
    tester_callback function(key, value) -> boolean
        Called with each key, value pair found in tree.
        If return value is true, delete entry. Otherwise, skip to next key.
    iter_kw dict
        Keyword arguments for tree.items .
        Warning: altered in this function.
    recycle_subtrees boolean (False)
        If true, deleted values will be put in TREE_POOL for future reuse.
        They must be BTrees.
        If False, values are not touched.
    """
    if iter_kw is None:
        iter_kw = {}
    if recycle_subtrees:
        deleter_callback = _btreeDeleterCallback
    else:
        deleter_callback = _deleterCallback
    items = tree.items
    while True:
        to_delete = []
        append = to_delete.append
        for key, value in safeIter(items, **iter_kw):
            if tester_callback(key, value):
                append(key)
                if len(to_delete) >= KEY_BATCH_SIZE:
                    iter_kw['min'] = key
                    iter_kw['excludemin'] = True
                    break
        if to_delete:
            deleter_callback(tree, to_delete)
        else:
            break

def _deleterCallback(tree, key_list):
    for key in key_list:
        del tree[key]

if hasattr(_OOBTree, 'pop'):
    def _btreeDeleterCallback(tree, key_list):
        for key in key_list:
            prune(tree.pop(key))
else:
    def _btreeDeleterCallback(tree, key_list):
        for key in key_list:
            prune(tree[key])
            del tree[key]

def OOBTree():
    try:
        result = TREE_POOL.pop()
    except IndexError:
        result = _OOBTree()
    # Next btree we prune will have room, restore prune method
    global prune
    prune = _prune
    return result

def _prune(tree):
    tree.clear()
    TREE_POOL.append(tree)
    if len(TREE_POOL) >= MAX_TREE_POOL_SIZE:
        # Already at/above max pool size, disable ourselve.
        global prune
        prune = _noPrune

def _noPrune(_):
    pass

prune = _prune

class CreationUndone(Exception):
    pass

def iterObjSerials(obj):
    for tserial in obj.values():
        for serial in tserial.keys():
            yield serial

def descItems(tree):
    try:
        key = tree.maxKey()
    except ValueError:
        pass
    else:
        while True:
            yield (key, tree[key])
            try:
                key = tree.maxKey(key - 1)
            except ValueError:
                break

def descKeys(tree):
    try:
        key = tree.maxKey()
    except ValueError:
        pass
    else:
        while True:
            yield key
            try:
                key = tree.maxKey(key - 1)
            except ValueError:
                break

def safeIter(func, *args, **kw):
    try:
        some_list = func(*args, **kw)
    except ValueError:
        some_list = []
    return some_list

class BTreeDatabaseManager(DatabaseManager):

    _obj = None
    _trans = None
    _tobj = None
    _ttrans = None
    _pt = None
    _config = None

    def __init__(self, database):
        super(BTreeDatabaseManager, self).__init__()
        self.setup(reset=1)

    def setup(self, reset=0):
        if reset:
            self._obj = OOBTree()
            self._trans = OOBTree()
            self.dropUnfinishedData()
            self._pt = {}
            self._config = {}

    def _begin(self):
        pass

    def _commit(self):
        pass

    def _rollback(self):
        pass

    def getConfiguration(self, key):
        return self._config[key]

    def _setConfiguration(self, key, value):
        self._config[key] = value

    def _setPackTID(self, tid):
        self._setConfiguration('_pack_tid', tid)

    def _getPackTID(self):
        try:
            result = int(self.getConfiguration('_pack_tid'))
        except KeyError:
            result = -1
        return result

    def getPartitionTable(self):
        pt = []
        append = pt.append
        for (offset, uuid), state in self._pt.iteritems():
            append((offset, uuid, state))
        return pt

    def getLastTID(self, all=True):
        try:
            ltid = self._trans.maxKey()
        except ValueError:
            ltid = None
        if all:
            try:
                tmp_ltid = self._ttrans.maxKey()
            except ValueError:
                tmp_ltid = None
            tmp_serial = None
            for tserial in self._tobj.values():
                try:
                    max_tmp_serial = tserial.maxKey()
                except ValueError:
                    pass
                else:
                    tmp_serial = max(tmp_serial, max_tmp_serial)
            ltid = max(ltid, tmp_ltid, tmp_serial)
        if ltid is not None:
            ltid = util.p64(ltid)
        return ltid

    def getUnfinishedTIDList(self):
        p64 = util.p64
        tid_set = set(p64(x) for x in self._ttrans.keys())
        tid_set.update(p64(x) for x in iterObjSerials(self._tobj))
        return list(tid_set)

    def objectPresent(self, oid, tid, all=True):
        u64 = util.u64
        oid = u64(oid)
        tid = u64(tid)
        try:
            result = self._obj[oid].has_key(tid)
        except KeyError:
            if all:
                try:
                    result = self._tobj[oid].has_key(tid)
                except KeyError:
                    result = False
            else:
                result = False
        return result

    def _getObjectData(self, oid, value_serial, tid):
        if value_serial is None:
            raise CreationUndone
        if value_serial >= tid:
            raise ValueError, "Incorrect value reference found for " \
                "oid %d at tid %d: reference = %d" % (oid, value_serial, tid)
        try:
            tserial = self._obj[oid]
        except KeyError:
            raise IndexError(oid)
        try:
            compression, checksum, value, next_value_serial = tserial[
                value_serial]
        except KeyError:
            raise IndexError(value_serial)
        if value is None:
            neo.lib.logging.info("Multiple levels of indirection when " \
                "searching for object data for oid %d at tid %d. This " \
                "causes suboptimal performance." % (oid, value_serial))
            value_serial, compression, checksum, value = self._getObjectData(
                oid, next_value_serial, value_serial)
        return value_serial, compression, checksum, value

    def _getObject(self, oid, tid=None, before_tid=None):
        tserial = self._obj.get(oid)
        if tserial is not None:
            if tid is None:
                try:
                    if before_tid is None:
                        tid = tserial.maxKey()
                    else:
                        tid = tserial.maxKey(before_tid - 1)
                except ValueError:
                    return
            result = tserial.get(tid)
            if result:
                try:
                    next_serial = tserial.minKey(tid + 1)
                except ValueError:
                    next_serial = None
                return (tid, next_serial) + result

    def doSetPartitionTable(self, ptid, cell_list, reset):
        pt = self._pt
        if reset:
            pt.clear()
        for offset, uuid, state in cell_list:
            # TODO: this logic should move out of database manager
            # add 'dropCells(cell_list)' to API and use one query
            key = (offset, uuid)
            if state == CellStates.DISCARDED:
                pt.pop(key, None)
            else:
                pt[key] = int(state)
        self.setPTID(ptid)

    def changePartitionTable(self, ptid, cell_list):
        self.doSetPartitionTable(ptid, cell_list, False)

    def setPartitionTable(self, ptid, cell_list):
        self.doSetPartitionTable(ptid, cell_list, True)

    def dropPartitions(self, num_partitions, offset_list):
        offset_list = frozenset(offset_list)
        def same_partition(key, _):
            return key % num_partitions in offset_list
        batchDelete(self._obj, same_partition, recycle_subtrees=True)
        batchDelete(self._trans, same_partition)

    def dropUnfinishedData(self):
        self._tobj = OOBTree()
        self._ttrans = OOBTree()

    def storeTransaction(self, tid, object_list, transaction, temporary=True):
        u64 = util.u64
        tid = u64(tid)
        if temporary:
            obj = self._tobj
            trans = self._ttrans
        else:
            obj = self._obj
            trans = self._trans
        for oid, compression, checksum, data, value_serial in object_list:
            oid = u64(oid)
            if data is None:
                compression = checksum = data
            else:
                # TODO: unit-test this raise
                if value_serial is not None:
                    raise ValueError, 'Either data or value_serial ' \
                        'must be None (oid %d, tid %d)' % (oid, tid)
            try:
                tserial = obj[oid]
            except KeyError:
                tserial = obj[oid] = OOBTree()
            if value_serial is not None:
                value_serial = u64(value_serial)
            tserial[tid] = (compression, checksum, data, value_serial)

        if transaction is not None:
            oid_list, user, desc, ext, packed = transaction
            trans[tid] = (tuple(oid_list), user, desc, ext, packed)

    def _getDataTIDFromData(self, oid, result):
        tid, _, _, _, data, value_serial = result
        if data is None:
            try:
                data_serial = self._getObjectData(oid, value_serial, tid)[0]
            except CreationUndone:
                data_serial = None
        else:
            data_serial = tid
        return tid, data_serial

    def _getDataTID(self, oid, tid=None, before_tid=None):
        result = self._getObject(oid, tid=tid, before_tid=before_tid)
        if result is None:
            result = (None, None)
        else:
            result = self._getDataTIDFromData(oid, result)
        return result

    def finishTransaction(self, tid):
        tid = util.u64(tid)
        self._popTransactionFromTObj(tid, True)
        ttrans = self._ttrans
        try:
            data = ttrans[tid]
        except KeyError:
            pass
        else:
            del ttrans[tid]
            self._trans[tid] = data

    def _popTransactionFromTObj(self, tid, to_obj):
        if to_obj:
            recycle_subtrees = False
            obj = self._obj
            def callback(oid, data):
                try:
                    tserial = obj[oid]
                except KeyError:
                    tserial = obj[oid] = OOBTree()
                tserial[tid] = data
        else:
            recycle_subtrees = True
            callback = lambda oid, data: None
        def tester_callback(oid, tserial):
            try:
                data = tserial[tid]
            except KeyError:
                pass
            else:
                del tserial[tid]
                callback(oid, data)
            return not tserial
        batchDelete(self._tobj, tester_callback,
            recycle_subtrees=recycle_subtrees)

    def deleteTransaction(self, tid, oid_list=()):
        tid = util.u64(tid)
        self._popTransactionFromTObj(tid, False)
        try:
            del self._ttrans[tid]
        except KeyError:
            pass
        for oid in oid_list:
            self._deleteObject(oid, serial=tid)
        try:
            del self._trans[tid]
        except KeyError:
            pass

    def deleteTransactionsAbove(self, num_partitions, partition, tid, max_tid):
        def same_partition(key, _):
            return key % num_partitions == partition
        batchDelete(self._trans, same_partition,
            iter_kw={'min': util.u64(tid), 'max': util.u64(max_tid)})

    def deleteObject(self, oid, serial=None):
        u64 = util.u64
        oid = u64(oid)
        if serial is not None:
            serial = u64(serial)
        self._deleteObject(oid, serial=serial)

    def _deleteObject(self, oid, serial=None):
        obj = self._obj
        try:
            tserial = obj[oid]
        except KeyError:
            pass
        else:
            if serial is not None:
                try:
                    del tserial[serial]
                except KeyError:
                    pass
            if serial is None or not tserial:
                prune(obj[oid])
                del obj[oid]

    def deleteObjectsAbove(self, num_partitions, partition, oid, serial,
                           max_tid):
        obj = self._obj
        u64 = util.u64
        oid = u64(oid)
        serial = u64(serial)
        max_tid = u64(max_tid)
        if oid % num_partitions == partition:
            try:
                tserial = obj[oid]
            except KeyError:
                pass
            else:
                batchDelete(tserial, lambda _, __: True,
                    iter_kw={'min': serial, 'max': max_tid})
        def same_partition(key, _):
            return key % num_partitions == partition
        batchDelete(obj, same_partition,
            iter_kw={'min': oid, 'excludemin': True, 'max': max_tid},
            recycle_subtrees=True)

    def getTransaction(self, tid, all=False):
        tid = util.u64(tid)
        try:
            result = self._trans[tid]
        except KeyError:
            if all:
                try:
                    result = self._ttrans[tid]
                except KeyError:
                    result = None
            else:
                result = None
        if result is not None:
            oid_list, user, desc, ext, packed = result
            result = (list(oid_list), user, desc, ext, packed)
        return result

    def getOIDList(self, min_oid, length, num_partitions,
            partition_list):
        p64 = util.p64
        partition_list = frozenset(partition_list)
        result = []
        append = result.append
        for oid in safeIter(self._obj.keys, min=min_oid):
            if oid % num_partitions in partition_list:
                if length == 0:
                    break
                length -= 1
                append(p64(oid))
        return result

    def _getObjectLength(self, oid, value_serial):
        if value_serial is None:
            raise CreationUndone
        _, _, value, value_serial = self._obj[oid][value_serial]
        if value is None:
            neo.lib.logging.info("Multiple levels of indirection when " \
                "searching for object data for oid %d at tid %d. This " \
                "causes suboptimal performance." % (oid, value_serial))
            length = self._getObjectLength(oid, value_serial)
        else:
            length = len(value)
        return length

    def getObjectHistory(self, oid, offset=0, length=1):
        # FIXME: This method doesn't take client's current ransaction id as
        # parameter, which means it can return transactions in the future of
        # client's transaction.
        oid = util.u64(oid)
        p64 = util.p64
        pack_tid = self._getPackTID()
        try:
            tserial = self._obj[oid]
        except KeyError:
            result = None
        else:
            result = []
            append = result.append
            tserial_iter = descItems(tserial)
            while offset > 0:
                tserial_iter.next()
                offset -= 1
            for serial, (_, _, value, value_serial) in tserial_iter:
                if length == 0 or serial < pack_tid:
                    break
                length -= 1
                if value is None:
                    try:
                        data_length = self._getObjectLength(oid, value_serial)
                    except CreationUndone:
                        data_length = 0
                else:
                    data_length = len(value)
                append((p64(serial), data_length))
        if not result:
            result = None
        return result

    def getObjectHistoryFrom(self, min_oid, min_serial, max_serial, length,
            num_partitions, partition):
        u64 = util.u64
        p64 = util.p64
        min_oid = u64(min_oid)
        min_serial = u64(min_serial)
        max_serial = u64(max_serial)
        result = {}
        for oid, tserial in safeIter(self._obj.items, min=min_oid):
            if oid % num_partitions == partition:
                if length == 0:
                    break
                if oid == min_oid:
                    try:
                        tid_seq = tserial.keys(min=min_serial,  max=max_serial)
                    except ValueError:
                        continue
                else:
                    tid_seq = tserial.keys(max=max_serial)
                if not tid_seq:
                    continue
                result[p64(oid)] = tid_list = []
                append = tid_list.append
                for tid in tid_seq:
                    if length == 0:
                        break
                    length -= 1
                    append(p64(tid))
                else:
                    continue
                break
        return result

    def getTIDList(self, offset, length, num_partitions, partition_list):
        p64 = util.p64
        partition_list = frozenset(partition_list)
        result = []
        append = result.append
        trans_iter = descKeys(self._trans)
        while offset > 0:
            tid = trans_iter.next()
            if tid % num_partitions in partition_list:
                offset -= 1
        for tid in trans_iter:
            if tid % num_partitions in partition_list:
                if length == 0:
                    break
                length -= 1
                append(p64(tid))
        return result

    def getReplicationTIDList(self, min_tid, max_tid, length, num_partitions,
            partition):
        p64 = util.p64
        u64 = util.u64
        result = []
        append = result.append
        for tid in safeIter(self._trans.keys, min=u64(min_tid), max=u64(max_tid)):
            if tid % num_partitions == partition:
                if length == 0:
                    break
                length -= 1
                append(p64(tid))
        return result

    def _updatePackFuture(self, oid, orig_serial, max_serial,
            updateObjectDataForPack):
        p64 = util.p64
        # Before deleting this objects revision, see if there is any
        # transaction referencing its value at max_serial or above.
        # If there is, copy value to the first future transaction. Any further
        # reference is just updated to point to the new data location.
        value_serial = None
        obj = self._obj
        for tree in (obj, self._tobj):
            try:
                tserial = tree[oid]
            except KeyError:
                continue
            for serial, record in tserial.items(
                    min=max_serial):
                if record[3] == orig_serial:
                    if value_serial is None:
                        value_serial = serial
                        tserial[serial] = tserial[orig_serial]
                    else:
                        record = list(record)
                        record[3] = value_serial
                        tserial[serial] = tuple(record)
        def getObjectData():
            assert value_serial is None
            return obj[oid][orig_serial][:3]
        if value_serial:
            value_serial = p64(value_serial)
        updateObjectDataForPack(p64(oid), p64(orig_serial), value_serial,
            getObjectData)

    def pack(self, tid, updateObjectDataForPack):
        tid = util.u64(tid)
        updatePackFuture = self._updatePackFuture
        self._setPackTID(tid)
        def obj_callback(oid, tserial):
            try:
                max_serial = tserial.maxKey(tid)
            except ValueError:
                # No entry before pack TID, nothing to pack on this object.
                pass
            else:
                if tserial[max_serial][2] == '':
                    # Last version before/at pack TID is a creation undo, drop
                    # it too.
                    max_serial += 1
                def serial_callback(serial, _):
                    updatePackFuture(oid, serial, max_serial,
                        updateObjectDataForPack)
                batchDelete(tserial, serial_callback,
                    iter_kw={'max': max_serial, 'excludemax': True})
            return not tserial
        batchDelete(self._obj, obj_callback, recycle_subtrees=True)

    def checkTIDRange(self, min_tid, max_tid, length, num_partitions, partition):
        if length:
            tid_list = []
            for tid in safeIter(self._trans.keys, min=util.u64(min_tid),
                                                  max=util.u64(max_tid)):
                if tid % num_partitions == partition:
                    tid_list.append(tid)
                    if len(tid_list) >= length:
                        break
            if tid_list:
                return (len(tid_list),
                        md5(','.join(map(str, tid_list))).digest(),
                        util.p64(tid_list[-1]))
        return 0, None, ZERO_TID

    def checkSerialRange(self, min_oid, min_serial, max_tid, length,
            num_partitions, partition):
        if length:
            u64 = util.u64
            min_oid = u64(min_oid)
            max_tid = u64(max_tid)
            oid_list = []
            serial_list = []
            for oid, tserial in safeIter(self._obj.items, min=min_oid):
                if oid % num_partitions == partition:
                    try:
                        if oid == min_oid:
                            tserial = tserial.keys(min=u64(min_serial),
                                                   max=max_tid)
                        else:
                            tserial = tserial.keys(max=max_tid)
                    except ValueError:
                        continue
                    for serial in tserial:
                        oid_list.append(oid)
                        serial_list.append(serial)
                        if len(oid_list) >= length:
                            break
                    else:
                        continue
                    break
            if oid_list:
                p64 = util.p64
                return (len(oid_list),
                        md5(','.join(map(str, oid_list))).digest(),
                        p64(oid_list[-1]),
                        md5(','.join(map(str, serial_list))).digest(),
                        p64(serial_list[-1]))
        return 0, None, ZERO_OID, None, ZERO_TID
