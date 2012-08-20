#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math, random, sys
from cStringIO import StringIO
from ZODB.utils import p64, u64
from ZODB.BaseStorage import TransactionRecord
from ZODB.FileStorage import FileStorage

# Stats of a 43.5 GB production Data.fs
#                          µ               σ
# size of object           6.04237779991   1.55811487853
# # objects / transaction  1.04108991045   0.906703192546
# size of transaction      7.98615420517   1.6624220402
#
# % of new object / transaction: 0.810080409164
# # of transactions: 1541194
# compression ratio: 28.5 % (gzip -6)
PROD1 = lambda random=random: DummyZODB(6.04237779991, 1.55811487853,
                                        1.04108991045, 0.906703192546,
                                        0.810080409164, random)

def DummyData(random=random):
    # returns data that gzip at about 28.5 %
    # make sure sample is bigger than dictionary of compressor
    data = ''.join(chr(int(random.gauss(0, .8)) % 256) for x in xrange(100000))
    return StringIO(data)


class DummyZODB(object):
    """
    Object size and count of generated transaction follows a log normal
    distribution, where *_mu and *_sigma are their parameters.
    """

    def __init__(self, obj_size_mu, obj_size_sigma,
                       obj_count_mu, obj_count_sigma,
                       new_ratio, random=random):
        self.obj_size_mu = obj_size_mu
        self.obj_size_sigma = obj_size_sigma
        self.obj_count_mu = obj_count_mu
        self.obj_count_sigma = obj_count_sigma
        self.random = random
        self.new_ratio = new_ratio
        self.next_oid = 0
        self.err_count = 0
        self.tid = u64('TID\0\0\0\0\0')

    def __call__(self):
        variate = self.random.lognormvariate
        oid_set = set()
        for i in xrange(int(round(variate(self.obj_count_mu,
                                          self.obj_count_sigma))) or 1):
            if len(oid_set) >= self.next_oid or \
               self.random.random() < self.new_ratio:
                oid = self.next_oid
                self.next_oid = oid + 1
            else:
                while True:
                    oid = self.random.randrange(self.next_oid)
                    if oid not in oid_set:
                        break
            oid_set.add(oid)
            yield p64(oid), int(round(variate(self.obj_size_mu,
                                              self.obj_size_sigma))) or 1

    def as_storage(self, stop, dummy_data_file=None):
        if dummy_data_file is None:
            dummy_data_file = DummyData(self.random)
        if isinstance(stop, int):
            stop = (lambda x: lambda y: x <= y)(stop)
        class dummy_change(object):
            data_txn = None
            version = ''
            def __init__(self, tid, oid, size):
                self.tid = tid
                self.oid = oid
                data = ''
                while size:
                    d = dummy_data_file.read(size)
                    size -= len(d)
                    data += d
                    if size:
                        dummy_data_file.seek(0)
                self.data = data
        class dummy_transaction(TransactionRecord):
            def __init__(transaction, *args):
                TransactionRecord.__init__(transaction, *args)
                transaction_size = 0
                transaction.record_list = []
                add_record = transaction.record_list.append
                for x in self():
                    oid, size = x
                    transaction_size += size
                    add_record(dummy_change(transaction.tid, oid, size))
                transaction.size = transaction_size
            def __iter__(transaction):
                return iter(transaction.record_list)
        class dummy_storage(object):
            size = 0
            def iterator(storage, *args):
                args = ' ', '', '', {}
                i = 0
                variate = self.random.lognormvariate
                while not stop(i):
                    self.tid += max(1, int(variate(10, 3)))
                    t =  dummy_transaction(p64(self.tid), *args)
                    storage.size += t.size
                    yield t
                    i += 1
            def getSize(self):
                return self.size
        return dummy_storage()

def lognorm_stat(X):
    Y = map(math.log, X)
    n = len(Y)
    mu = sum(Y) / n
    s2 = sum(d*d for d in (y - mu for y in Y)) / n
    return mu, math.sqrt(s2)

def stat(*storages):
    obj_size_list = []
    obj_count_list = []
    tr_size_list = []
    oid_set = set()
    for storage in storages:
        for transaction in storage.iterator():
            obj_count = tr_size = 0
            for r in transaction:
                if r.data:
                    obj_count += 1
                    oid = r.oid
                    if oid not in oid_set:
                        oid_set.add(oid)
                    size = len(r.data)
                    tr_size += size
                    obj_size_list.append(size)
            obj_count_list.append(obj_count)
            tr_size_list.append(tr_size)
    new_ratio = float(len(oid_set)) / len(obj_size_list)
    return (lognorm_stat(obj_size_list),
            lognorm_stat(obj_count_list),
            lognorm_stat(tr_size_list),
            new_ratio, len(tr_size_list))

def main():
    s = stat(*(FileStorage(x, read_only=True) for x in sys.argv[1:]))
    print(u"                         %-15s σ\n"
           "size of object           %-15s %s\n"
           "# objects / transaction  %-15s %s\n"
           "size of transaction      %-15s %s\n"
           "\n%% of new object / transaction: %s"
           "\n# of transactions: %s"
           % ((u"µ",) + s[0] + s[1] + s[2] + s[3:]))


if __name__ == "__main__":
    sys.exit(main())
