##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import unittest

import ZODB
import ZODB.FileStorage
from ZODB.POSException import ReadConflictError, ConflictError
from ZODB.POSException import TransactionFailedError
from ZODB.tests.warnhook import WarningsHook

from persistent import Persistent
from persistent.mapping import PersistentMapping
import transaction

from neo.client.Storage import Storage
import os
import sys
import signal
import MySQLdb
import logging
import tempfile
import traceback

class P(Persistent):
    pass

class Independent(Persistent):

    def _p_independent(self):
        return 1

class DecoyIndependent(Persistent):

    def _p_independent(self):
        return 0

# No need to protect this list with a lock, NEOProcess instanciations and
# killallNeo calls are done from a single thread.
neo_process_list = []

class NEOProcess:
    pid = 0

    def __init__(self, command, *args):
        self.pid = os.fork()
        if self.pid == 0:
            # Child
            try:
                os.execlp(command, command, *args)
            except:
                print traceback.format_exc()
            # If we reach this line, exec call failed (is it possible to reach
            # it without going through above "except" branch ?).
            print 'Error executing %r.' % (command + ' '.join(args), )
            # KeyboardInterrupt is not intercepted by test runner (it is still
            # above us in the stack), and we do want to exit.
            # To avoid polluting test foreground output with induced
            # traceback, replace stdout & stderr.
            sys.stdout = sys.stderr = open('/dev/null', 'w')
            raise KeyboardInterrupt
        else:
            neo_process_list.append(self)

    def kill(self, sig=signal.SIGTERM):
        if self.pid:
            try:
                os.kill(self.pid, sig)
            except OSError:
                traceback.print_last()

    def __del__(self):
        # If we get killed, kill subprocesses aswell.
        try:
            self.kill(signal.SIGKILL)
        except:
            # We can ignore all exceptions at this point, since there is no
            # garanteed way to handle them (other objects we would depend on
            # might already have been deleted).
            pass

    def wait(self, options=0):
        assert self.pid
        return os.WEXITSTATUS(os.waitpid(self.pid, options)[1])

def killallNeo():
    while len(neo_process_list):
        process = neo_process_list.pop()
        process.kill()
        process.wait()

NEO_MASTER = 'neomaster'
NEO_STORAGE = 'neostorage'
NEO_PORT_BASE = 10010
NEO_CLUSTER_NAME = 'test'
NEO_MASTER_PORT_1 = NEO_PORT_BASE
NEO_MASTER_PORT_2 = NEO_MASTER_PORT_1 + 1
NEO_MASTER_PORT_3 = NEO_MASTER_PORT_2 + 1
NEO_STORAGE_PORT_1 = NEO_MASTER_PORT_3 + 1
NEO_STORAGE_PORT_2 = NEO_STORAGE_PORT_1 + 1
NEO_STORAGE_PORT_3 = NEO_STORAGE_PORT_2 + 1
NEO_STORAGE_PORT_4 = NEO_STORAGE_PORT_3 + 1
NEO_MASTER_NODES = '127.0.0.1:%(port_1)s 127.0.0.1:%(port_2)s 127.0.0.1:%(port_3)s' % {
    'port_1': NEO_MASTER_PORT_1,
    'port_2': NEO_MASTER_PORT_2,
    'port_3': NEO_MASTER_PORT_3
}
NEO_SQL_USER = 'test'
NEO_SQL_PASSWORD = ''
NEO_SQL_DATABASE_1 = 'test_neo1'
NEO_SQL_DATABASE_2 = 'test_neo2'
NEO_SQL_DATABASE_3 = 'test_neo3'
NEO_SQL_DATABASE_4 = 'test_neo4'
# Used to create & drop above databases and grant test users privileges.
SQL_ADMIN_USER = 'root'
SQL_ADMIN_PASSWORD = None

NEO_CONFIG = '''
# Default parameters.
[DEFAULT]
# The list of master nodes.
master_nodes: %(master_nodes)s
# The number of replicas.
replicas: 2
# The number of partitions.
partitions: 1009
# The name of this cluster.
name: %(name)s
# The user name for the database.
user: %(user)s
# The password for the database.
password: %(password)s
# The connector class used
connector: SocketConnector

# The first master.
[master1]
server: 127.0.0.1:%(master1_port)s

# The second master.
[master2]
server: 127.0.0.1:%(master2_port)s

# The third master.
[master3]
server: 127.0.0.1:%(master3_port)s

# The first storage.
[storage1]
database: %(storage1_db)s
server: 127.0.0.1:%(storage1_port)s

# The first storage.
[storage2]
database: %(storage2_db)s
server: 127.0.0.1:%(storage2_port)s

# The third storage.
[storage3]
database: %(storage3_db)s
server: 127.0.0.1:%(storage3_port)s

# The fourth storage.
[storage4]
database: %(storage4_db)s
server: 127.0.0.1:%(storage4_port)s
''' % {
    'master_nodes': NEO_MASTER_NODES,
    'name': NEO_CLUSTER_NAME,
    'user': NEO_SQL_USER,
    'password': NEO_SQL_PASSWORD,
    'master1_port': NEO_MASTER_PORT_1,
    'master2_port': NEO_MASTER_PORT_2,
    'master3_port': NEO_MASTER_PORT_3,
    'storage1_port': NEO_STORAGE_PORT_1,
    'storage1_db': NEO_SQL_DATABASE_1,
    'storage2_port': NEO_STORAGE_PORT_2,
    'storage2_db': NEO_SQL_DATABASE_2,
    'storage3_port': NEO_STORAGE_PORT_3,
    'storage3_db': NEO_SQL_DATABASE_3,
    'storage4_port': NEO_STORAGE_PORT_4,
    'storage4_db': NEO_SQL_DATABASE_4
}
temp_dir = tempfile.mkdtemp(prefix='neo_')
print 'Using temp directory %r.' % (temp_dir, )
config_file_path = os.path.join(temp_dir, 'neo.conf')
config_file = open(config_file_path, 'w')
config_file.write(NEO_CONFIG)
config_file.close()
m1_log = os.path.join(temp_dir, 'm1.log')
m2_log = os.path.join(temp_dir, 'm2.log')
m3_log = os.path.join(temp_dir, 'm3.log')
s1_log = os.path.join(temp_dir, 's1.log')
s2_log = os.path.join(temp_dir, 's2.log')
s3_log = os.path.join(temp_dir, 's3.log')
s4_log = os.path.join(temp_dir, 's4.log')
# override logging default handler
from neo import buildFormatString
client_log = os.path.join(temp_dir, 'c.log')
format = buildFormatString('CLIENT')
logging.basicConfig(filename=client_log, level=logging.DEBUG, format=format)

class ZODBTests(unittest.TestCase):

    def setUp(self):
        # Stop NEO cluster (if running)
        killallNeo()
        # Cleanup or bootstrap databases
        connect_arg_dict = {'user': SQL_ADMIN_USER}
        if SQL_ADMIN_PASSWORD is not None:
            connect_arg_dict['passwd'] = SQL_ADMIN_PASSWORD
        sql_connection = MySQLdb.Connect(**connect_arg_dict)
        cursor = sql_connection.cursor()
        for database in (NEO_SQL_DATABASE_1, NEO_SQL_DATABASE_2, NEO_SQL_DATABASE_3, NEO_SQL_DATABASE_4):
            cursor.execute('DROP DATABASE IF EXISTS %s' % (database, ))
            cursor.execute('CREATE DATABASE %s' % (database, ))
            cursor.execute('GRANT ALL ON %s.* TO "%s"@"localhost" IDENTIFIED BY "%s"' % (database, NEO_SQL_USER, NEO_SQL_PASSWORD))
        cursor.close()
        sql_connection.close()
        # Start NEO cluster
        NEOProcess(NEO_MASTER, '-vc', config_file_path, '-s', 'master1', '-l', m1_log)
        NEOProcess(NEO_MASTER, '-vc', config_file_path, '-s', 'master2', '-l', m2_log)
        NEOProcess(NEO_MASTER, '-vc', config_file_path, '-s', 'master3', '-l', m3_log)
        NEOProcess(NEO_STORAGE, '-vRc', config_file_path, '-s', 'storage1', '-l', s1_log)
        NEOProcess(NEO_STORAGE, '-vRc', config_file_path, '-s', 'storage2', '-l', s2_log)
        NEOProcess(NEO_STORAGE, '-vRc', config_file_path, '-s', 'storage3', '-l', s3_log)
        NEOProcess(NEO_STORAGE, '-vRc', config_file_path, '-s', 'storage4', '-l', s4_log)
        # Send Storage output to a logfile
        self._storage = Storage(
            master_nodes=NEO_MASTER_NODES,
            name=NEO_CLUSTER_NAME,
            connector='SocketConnector')
        self._db = ZODB.DB(self._storage)

    def populate(self):
        transaction.begin()
        conn = self._db.open()
        root = conn.root()
        root['test'] = pm = PersistentMapping()
        for n in range(100):
            pm[n] = PersistentMapping({0: 100 - n})
        transaction.get().note('created test data')
        transaction.commit()
        conn.close()

    def tearDown(self):
        self._db.close()
        self._storage.cleanup()
        killallNeo()

    def checkExportImport(self, abort_it=False):
        self.populate()
        conn = self._db.open()
        try:
            self.duplicate(conn, abort_it)
        finally:
            conn.close()
        conn = self._db.open()
        try:
            self.verify(conn, abort_it)
        finally:
            conn.close()

    def duplicate(self, conn, abort_it):
        transaction.begin()
        transaction.get().note('duplication')
        root = conn.root()
        ob = root['test']
        assert len(ob) > 10, 'Insufficient test data'
        try:
            import tempfile
            f = tempfile.TemporaryFile()
            ob._p_jar.exportFile(ob._p_oid, f)
            assert f.tell() > 0, 'Did not export correctly'
            f.seek(0)
            new_ob = ob._p_jar.importFile(f)
            self.assertEqual(new_ob, ob)
            root['dup'] = new_ob
            f.close()
            if abort_it:
                transaction.abort()
            else:
                transaction.commit()
        except:
            transaction.abort()
            raise

    def verify(self, conn, abort_it):
        transaction.begin()
        root = conn.root()
        ob = root['test']
        try:
            ob2 = root['dup']
        except KeyError:
            if abort_it:
                # Passed the test.
                return
            else:
                raise
        else:
            self.failUnless(not abort_it, 'Did not abort duplication')
        l1 = list(ob.items())
        l1.sort()
        l2 = list(ob2.items())
        l2.sort()
        l1 = map(lambda (k, v): (k, v[0]), l1)
        l2 = map(lambda (k, v): (k, v[0]), l2)
        self.assertEqual(l1, l2)
        self.assert_(ob._p_oid != ob2._p_oid)
        self.assertEqual(ob._p_jar, ob2._p_jar)
        oids = {}
        for v in ob.values():
            oids[v._p_oid] = 1
        for v in ob2.values():
            assert not oids.has_key(v._p_oid), (
                'Did not fully separate duplicate from original')
        transaction.commit()

    def checkExportImportAborted(self):
        self.checkExportImport(abort_it=True)

    def checkVersionOnly(self):
        # Make sure the changes to make empty transactions a no-op
        # still allow things like abortVersion().  This should work
        # because abortVersion() calls tpc_begin() itself.
        conn = self._db.open("version")
        try:
            r = conn.root()
            r[1] = 1
            transaction.commit()
        finally:
            conn.close()
        self._db.abortVersion("version")
        transaction.commit()

    def checkResetCache(self):
        # The cache size after a reset should be 0.  Note that
        # _resetCache is not a public API, but the resetCaches()
        # function is, and resetCaches() causes _resetCache() to be
        # called.
        self.populate()
        conn = self._db.open()
        conn.root()
        self.assert_(len(conn._cache) > 0)  # Precondition
        conn._resetCache()
        self.assertEqual(len(conn._cache), 0)

    def checkResetCachesAPI(self):
        # Checks the resetCaches() API.
        # (resetCaches used to be called updateCodeTimestamp.)
        self.populate()
        conn = self._db.open()
        conn.root()
        self.assert_(len(conn._cache) > 0)  # Precondition
        ZODB.Connection.resetCaches()
        conn.close()
        self.assert_(len(conn._cache) > 0)  # Still not flushed
        conn._setDB(self._db)  # simulate the connection being reopened
        self.assertEqual(len(conn._cache), 0)

    def checkExplicitTransactionManager(self):
        # Test of transactions that apply to only the connection,
        # not the thread.
        tm1 = transaction.TransactionManager()
        conn1 = self._db.open(transaction_manager=tm1)
        tm2 = transaction.TransactionManager()
        conn2 = self._db.open(transaction_manager=tm2)
        try:
            r1 = conn1.root()
            r2 = conn2.root()
            if r1.has_key('item'):
                del r1['item']
                tm1.get().commit()
            r1.get('item')
            r2.get('item')
            r1['item'] = 1
            tm1.get().commit()
            self.assertEqual(r1['item'], 1)
            # r2 has not seen a transaction boundary,
            # so it should be unchanged.
            self.assertEqual(r2.get('item'), None)
            conn2.sync()
            # Now r2 is updated.
            self.assertEqual(r2['item'], 1)

            # Now, for good measure, send an update in the other direction.
            r2['item'] = 2
            tm2.get().commit()
            self.assertEqual(r1['item'], 1)
            self.assertEqual(r2['item'], 2)
            conn1.sync()
            conn2.sync()
            self.assertEqual(r1['item'], 2)
            self.assertEqual(r2['item'], 2)
        finally:
            conn1.close()
            conn2.close()

    def checkLocalTransactions(self):
        # Test of transactions that apply to only the connection,
        # not the thread.
        conn1 = self._db.open()
        conn2 = self._db.open()
        hook = WarningsHook()
        hook.install()
        try:
            conn1.setLocalTransaction()
            conn2.setLocalTransaction()
            r1 = conn1.root()
            r2 = conn2.root()
            if r1.has_key('item'):
                del r1['item']
                conn1.getTransaction().commit()
            r1.get('item')
            r2.get('item')
            r1['item'] = 1
            conn1.getTransaction().commit()
            self.assertEqual(r1['item'], 1)
            # r2 has not seen a transaction boundary,
            # so it should be unchanged.
            self.assertEqual(r2.get('item'), None)
            conn2.sync()
            # Now r2 is updated.
            self.assertEqual(r2['item'], 1)

            # Now, for good measure, send an update in the other direction.
            r2['item'] = 2
            conn2.getTransaction().commit()
            self.assertEqual(r1['item'], 1)
            self.assertEqual(r2['item'], 2)
            conn1.sync()
            conn2.sync()
            self.assertEqual(r1['item'], 2)
            self.assertEqual(r2['item'], 2)
            for msg, obj, filename, lineno in hook.warnings:
                self.assert_(msg in [
                    "This will be removed in ZODB 3.6:\n"
                        "setLocalTransaction() is deprecated. "
                        "Use the transaction_manager argument "
                        "to DB.open() instead.",
                    "This will be removed in ZODB 3.6:\n"
                        "getTransaction() is deprecated. "
                        "Use the transaction_manager argument "
                        "to DB.open() instead, or access "
                        ".transaction_manager directly on the Connection."])
        finally:
            conn1.close()
            conn2.close()
            hook.uninstall()

    def checkReadConflict(self):
        self.obj = P()
        self.readConflict()

    def readConflict(self, shouldFail=True):
        # Two transactions run concurrently.  Each reads some object,
        # then one commits and the other tries to read an object
        # modified by the first.  This read should fail with a conflict
        # error because the object state read is not necessarily
        # consistent with the objects read earlier in the transaction.

        tm1 = transaction.TransactionManager()
        conn = self._db.open(mvcc=False, transaction_manager=tm1)
        r1 = conn.root()
        r1["p"] = self.obj
        self.obj.child1 = P()
        tm1.get().commit()

        # start a new transaction with a new connection
        tm2 = transaction.TransactionManager()
        cn2 = self._db.open(mvcc=False, transaction_manager=tm2)
        # start a new transaction with the other connection
        r2 = cn2.root()

        self.assertEqual(r1._p_serial, r2._p_serial)

        self.obj.child2 = P()
        tm1.get().commit()

        # resume the transaction using cn2
        obj = r2["p"]
        # An attempt to access obj should fail, because r2 was read
        # earlier in the transaction and obj was modified by the othe
        # transaction.
        if shouldFail:
            self.assertRaises(ReadConflictError, lambda: obj.child1)
            # And since ReadConflictError was raised, attempting to commit
            # the transaction should re-raise it.  checkNotIndependent()
            # failed this part of the test for a long time.
            self.assertRaises(ReadConflictError, tm2.get().commit)

            # And since that commit failed, trying to commit again should
            # fail again.
            self.assertRaises(TransactionFailedError, tm2.get().commit)
            # And again.
            self.assertRaises(TransactionFailedError, tm2.get().commit)
            # Etc.
            self.assertRaises(TransactionFailedError, tm2.get().commit)

        else:
            # make sure that accessing the object succeeds
            obj.child1
        tm2.get().abort()

    def checkReadConflictIgnored(self):
        # Test that an application that catches a read conflict and
        # continues can not commit the transaction later.
        root = self._db.open(mvcc=False).root()
        root["real_data"] = real_data = PersistentMapping()
        root["index"] = index = PersistentMapping()

        real_data["a"] = PersistentMapping({"indexed_value": 0})
        real_data["b"] = PersistentMapping({"indexed_value": 1})
        index[1] = PersistentMapping({"b": 1})
        index[0] = PersistentMapping({"a": 1})
        transaction.commit()

        # load some objects from one connection
        tm = transaction.TransactionManager()
        cn2 = self._db.open(mvcc=False, transaction_manager=tm)
        r2 = cn2.root()
        real_data2 = r2["real_data"]
        index2 = r2["index"]

        real_data["b"]["indexed_value"] = 0
        del index[1]["b"]
        index[0]["b"] = 1
        transaction.commit()

        del real_data2["a"]
        try:
            del index2[0]["a"]
        except ReadConflictError:
            # This is the crux of the text.  Ignore the error.
            pass
        else:
            self.fail("No conflict occurred")

        # real_data2 still ready to commit
        self.assert_(real_data2._p_changed)

        # index2 values not ready to commit
        self.assert_(not index2._p_changed)
        self.assert_(not index2[0]._p_changed)
        self.assert_(not index2[1]._p_changed)

        self.assertRaises(ReadConflictError, tm.get().commit)
        self.assertRaises(TransactionFailedError, tm.get().commit)
        tm.get().abort()

    def checkIndependent(self):
        self.obj = Independent()
        self.readConflict(shouldFail=False)

    def checkNotIndependent(self):
        self.obj = DecoyIndependent()
        self.readConflict()

    def checkSubtxnCommitDoesntGetInvalidations(self):
        # Prior to ZODB 3.2.9 and 3.4, Connection.tpc_finish() processed
        # invalidations even for a subtxn commit.  This could make
        # inconsistent state visible after a subtxn commit.  There was a
        # suspicion that POSKeyError was possible as a result, but I wasn't
        # able to construct a case where that happened.

        # Set up the database, to hold
        # root --> "p" -> value = 1
        #      --> "q" -> value = 2
        tm1 = transaction.TransactionManager()
        conn = self._db.open(transaction_manager=tm1)
        r1 = conn.root()
        p = P()
        p.value = 1
        r1["p"] = p
        q = P()
        q.value = 2
        r1["q"] = q
        tm1.commit()

        # Now txn T1 changes p.value to 3 locally (subtxn commit).
        p.value = 3
        tm1.commit(True)

        # Start new txn T2 with a new connection.
        tm2 = transaction.TransactionManager()
        cn2 = self._db.open(transaction_manager=tm2)
        r2 = cn2.root()
        p2 = r2["p"]
        self.assertEqual(p._p_oid, p2._p_oid)
        # T2 shouldn't see T1's change of p.value to 3, because T1 didn't
        # commit yet.
        self.assertEqual(p2.value, 1)
        # Change p.value to 4, and q.value to 5.  Neither should be visible
        # to T1, because T1 is still in progress.
        p2.value = 4
        q2 = r2["q"]
        self.assertEqual(q._p_oid, q2._p_oid)
        self.assertEqual(q2.value, 2)
        q2.value = 5
        tm2.commit()

        # Back to T1.  p and q still have the expected values.
        rt = conn.root()
        self.assertEqual(rt["p"].value, 3)
        self.assertEqual(rt["q"].value, 2)

        # Now do another subtxn commit in T1.  This shouldn't change what
        # T1 sees for p and q.
        rt["r"] = P()
        tm1.commit(True)

        # Doing that subtxn commit in T1 should not process invalidations
        # from T2's commit.  p.value should still be 3 here (because that's
        # what T1 subtxn-committed earlier), and q.value should still be 2.
        # Prior to ZODB 3.2.9 and 3.4, q.value was 5 here.
        rt = conn.root()
        try:
            self.assertEqual(rt["p"].value, 3)
            self.assertEqual(rt["q"].value, 2)
        finally:
            tm1.abort()

    def checkReadConflictErrorClearedDuringAbort(self):
        # When a transaction is aborted, the "memory" of which
        # objects were the cause of a ReadConflictError during
        # that transaction should be cleared.
        root = self._db.open(mvcc=False).root()
        data = PersistentMapping({'d': 1})
        root["data"] = data
        transaction.commit()

        # Provoke a ReadConflictError.
        tm2 = transaction.TransactionManager()
        cn2 = self._db.open(mvcc=False, transaction_manager=tm2)
        r2 = cn2.root()
        data2 = r2["data"]

        data['d'] = 2
        transaction.commit()

        try:
            data2['d'] = 3
        except ReadConflictError:
            pass
        else:
            self.fail("No conflict occurred")

        # Explicitly abort cn2's transaction.
        tm2.get().abort()

        # cn2 should retain no memory of the read conflict after an abort(),
        # but 3.2.3 had a bug wherein it did.
        data_conflicts = data._p_jar._conflicts
        data2_conflicts = data2._p_jar._conflicts
        self.failIf(data_conflicts)
        self.failIf(data2_conflicts)  # this used to fail

        # And because of that, we still couldn't commit a change to data2['d']
        # in the new transaction.
        cn2.sync()  # process the invalidation for data2['d']
        data2['d'] = 3
        tm2.get().commit()  # 3.2.3 used to raise ReadConflictError

        cn2.close()

    def checkTxnBeginImpliesAbort(self):
        # begin() should do an abort() first, if needed.
        cn = self._db.open()
        rt = cn.root()
        rt['a'] = 1

        transaction.begin()  # should abort adding 'a' to the root
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')

        # A longstanding bug:  this didn't work if changes were only in
        # subtransactions.
        transaction.begin()
        rt = cn.root()
        rt['a'] = 2
        transaction.commit(1)

        transaction.begin()
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')

        # One more time, mixing "top level" and subtransaction changes.
        transaction.begin()
        rt = cn.root()
        rt['a'] = 3
        transaction.commit(1)
        rt['b'] = 4

        transaction.begin()
        rt = cn.root()
        self.assertRaises(KeyError, rt.__getitem__, 'a')
        self.assertRaises(KeyError, rt.__getitem__, 'b')

        # That used methods of the default transaction *manager*.  Alas,
        # that's not necessarily the same as using methods of the current
        # transaction, and, in fact, when this test was written,
        # Transaction.begin() didn't do anything (everything from here
        # down failed).

        # Oh, bleech.  Since Transaction.begin is also deprecated, we have
        # to goof around suppressing the deprecation warning.
        import warnings

        # First verify that Transaction.begin *is* deprecated, by turning
        # the warning into an error.
        warnings.filterwarnings("error", category=DeprecationWarning)
        self.assertRaises(DeprecationWarning, transaction.get().begin)
        del warnings.filters[0]

        # Now ignore DeprecationWarnings for the duration.  Use a
        # try/finally block to ensure we reenable DeprecationWarnings
        # no matter what.
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        try:
            cn = self._db.open()
            rt = cn.root()
            rt['a'] = 1

            transaction.get().begin()  # should abort adding 'a' to the root
            rt = cn.root()
            self.assertRaises(KeyError, rt.__getitem__, 'a')

            # A longstanding bug:  this didn't work if changes were only in
            # subtransactions.
            transaction.get().begin()
            rt = cn.root()
            rt['a'] = 2
            transaction.get().commit(1)

            transaction.get().begin()
            rt = cn.root()
            self.assertRaises(KeyError, rt.__getitem__, 'a')

            # One more time, mixing "top level" and subtransaction changes.
            transaction.get().begin()
            rt = cn.root()
            rt['a'] = 3
            transaction.get().commit(1)
            rt['b'] = 4

            transaction.get().begin()
            rt = cn.root()
            self.assertRaises(KeyError, rt.__getitem__, 'a')
            self.assertRaises(KeyError, rt.__getitem__, 'b')

            cn.close()

        finally:
            del warnings.filters[0]

    def checkFailingCommitSticks(self):
        # See also checkFailingSubtransactionCommitSticks.
        cn = self._db.open()
        rt = cn.root()
        rt['a'] = 1

        # Arrange for commit to fail during tpc_vote.
        poisoned = PoisonedObject(PoisonedJar(break_tpc_vote=True))
        transaction.get().register(poisoned)

        self.assertRaises(PoisonedError, transaction.get().commit)
        # Trying to commit again fails too.
        self.assertRaises(TransactionFailedError, transaction.get().commit)
        self.assertRaises(TransactionFailedError, transaction.get().commit)
        self.assertRaises(TransactionFailedError, transaction.get().commit)

        # The change to rt['a'] is lost.
        self.assertRaises(KeyError, rt.__getitem__, 'a')

        # Trying to modify an object also fails, because Transaction.join()
        # also raises TransactionFailedError.
        self.assertRaises(TransactionFailedError, rt.__setitem__, 'b', 2)

        # Clean up via abort(), and try again.
        transaction.get().abort()
        rt['a'] = 1
        transaction.get().commit()
        self.assertEqual(rt['a'], 1)

        # Cleaning up via begin() should also work.
        rt['a'] = 2
        transaction.get().register(poisoned)
        self.assertRaises(PoisonedError, transaction.get().commit)
        self.assertRaises(TransactionFailedError, transaction.get().commit)
        # The change to rt['a'] is lost.
        self.assertEqual(rt['a'], 1)
        # Trying to modify an object also fails.
        self.assertRaises(TransactionFailedError, rt.__setitem__, 'b', 2)
        # Clean up via begin(), and try again.
        transaction.begin()
        rt['a'] = 2
        transaction.get().commit()
        self.assertEqual(rt['a'], 2)

        cn.close()

    def checkFailingSubtransactionCommitSticks(self):
        cn = self._db.open()
        rt = cn.root()
        rt['a'] = 1
        transaction.get().commit(True)
        self.assertEqual(rt['a'], 1)

        rt['b'] = 2

        # Subtransactions don't do tpc_vote, so we poison tpc_begin.
        poisoned = PoisonedJar()
        transaction.get().join(poisoned)
        poisoned.break_savepoint = True
        self.assertRaises(PoisonedError, transaction.get().commit, True)
        # Trying to subtxn-commit again fails too.
        self.assertRaises(TransactionFailedError,
                          transaction.get().commit, True)
        self.assertRaises(TransactionFailedError,
                          transaction.get().commit, True)
        # Top-level commit also fails.
        self.assertRaises(TransactionFailedError, transaction.get().commit)

        # The changes to rt['a'] and rt['b'] are lost.
        self.assertRaises(KeyError, rt.__getitem__, 'a')
        self.assertRaises(KeyError, rt.__getitem__, 'b')

        # Trying to modify an object also fails, because Transaction.join()
        # also raises TransactionFailedError.
        self.assertRaises(TransactionFailedError, rt.__setitem__, 'b', 2)


        # Clean up via abort(), and try again.
        transaction.get().abort()
        rt['a'] = 1
        transaction.get().commit()
        self.assertEqual(rt['a'], 1)

        # Cleaning up via begin() should also work.
        rt['a'] = 2
        poisoned = PoisonedJar()
        transaction.get().join(poisoned)
        poisoned.break_savepoint = True
        self.assertRaises(PoisonedError, transaction.get().commit, True)
        self.assertRaises(TransactionFailedError,
                          transaction.get().commit, True)

        # The change to rt['a'] is lost.
        self.assertEqual(rt['a'], 1)
        # Trying to modify an object also fails.
        self.assertRaises(TransactionFailedError, rt.__setitem__, 'b', 2)

        # Clean up via begin(), and try again.
        transaction.begin()
        rt['a'] = 2
        transaction.get().commit(True)
        self.assertEqual(rt['a'], 2)
        transaction.get().commit()

        cn2 = self._db.open()
        rt = cn.root()
        self.assertEqual(rt['a'], 2)

        cn.close()
        cn2.close()

class PoisonedError(Exception):
    pass

# PoisonedJar arranges to raise exceptions from interesting places.
# For whatever reason, subtransaction commits don't call tpc_vote.
class PoisonedJar:
    def __init__(self, break_tpc_begin=False, break_tpc_vote=False,
                 break_savepoint=False):
        self.break_tpc_begin = break_tpc_begin
        self.break_tpc_vote = break_tpc_vote
        self.break_savepoint = break_savepoint

    def sortKey(self):
        return str(id(self))

    # A way to poison a subtransaction commit.
    def tpc_begin(self, *args):
        if self.break_tpc_begin:
            raise PoisonedError("tpc_begin fails")

    # A way to poison a top-level commit.
    def tpc_vote(self, *args):
        if self.break_tpc_vote:
            raise PoisonedError("tpc_vote fails")

    def savepoint(self):
        if self.break_savepoint:
            raise PoisonedError("savepoint fails")

    def commit(*args):
        pass

    def abort(*self):
        pass


class PoisonedObject:
    def __init__(self, poisonedjar):
        self._p_jar = poisonedjar

def test_suite():
    return unittest.makeSuite(ZODBTests, 'check')

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
