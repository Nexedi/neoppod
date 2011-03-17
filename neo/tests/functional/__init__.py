#
# Copyright (C) 2009-2010  Nexedi SA
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

import os
import sys
import time
import ZODB
import socket
import signal
import random
import weakref
import MySQLdb
import unittest
import tempfile
import traceback
import threading

from neo.neoctl.neoctl import NeoCTL, NotReadyException
from neo.lib.protocol import ClusterStates, NodeTypes, CellStates, NodeStates
from neo.lib.util import dump, SOCKET_CONNECTORS_DICT
from neo.tests import DB_ADMIN, DB_PASSWD, NeoTestBase, buildUrlFromString, \
        ADDRESS_TYPE, IP_VERSION_FORMAT_DICT, SocketLock, getTempDirectory
from neo.client.Storage import Storage

NEO_MASTER = 'neomaster'
NEO_STORAGE = 'neostorage'
NEO_ADMIN = 'neoadmin'

DELAY_SAFETY_MARGIN = 10
MAX_START_TIME = 30

class NodeProcessError(Exception):
    pass

class AlreadyRunning(Exception):
    pass

class AlreadyStopped(Exception):
    pass

class NotFound(Exception):
    pass

class PortAllocator(object):

    lock = SocketLock('neo.PortAllocator')
    allocator_set = weakref.WeakKeyDictionary() # BBB: use WeakSet instead

    def __init__(self):
        self.socket_list = []

    def allocate(self, address_type, local_ip):
        s = socket.socket(address_type, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if not self.lock.locked():
            self.lock.acquire()
        self.allocator_set[self] = None
        self.socket_list.append(s)
        s.bind((local_ip, 0))
        return s.getsockname()[1]

    def release(self):
        for s in self.socket_list:
            s.close()
        self.socket_list = None

    def reset(self):
        if self.lock.locked():
            self.allocator_set.pop(self, None)
            if not self.allocator_set:
                self.lock.release()
            if self.socket_list:
                for s in self.socket_list:
                    s.close()
            self.__init__()

    __del__ = reset

class NEOProcess(object):
    pid = 0

    def __init__(self, command, uuid, arg_dict):
        path = os.getenv('PATH')
        split_path = path.split(":")

        for elt_path in split_path:
            command_tmp = "%s/%s" % (elt_path, command)
            if os.path.exists(command_tmp):
                self.command = command_tmp
                break
        else:
            raise NotFound, '%s not found' % (command)

        self.arg_dict = arg_dict
        self.with_uuid = True
        self.setUUID(uuid)

    def start(self, with_uuid=True):
        # Prevent starting when already forked and wait wasn't called.
        if self.pid != 0:
            raise AlreadyRunning, 'Already running with PID %r' % (self.pid, )
        command = self.command
        args = []
        self.with_uuid = with_uuid
        for arg, param in self.arg_dict.iteritems():
            if with_uuid is False and arg == '--uuid':
                continue
            args.append(arg)
            if param is not None:
                args.append(str(param))
        self.pid = os.fork()
        if self.pid == 0:
            # Child
            try:
                sys.argv = [command] + args
                execfile(command, {"__name__": "__main__"})
            except (SystemExit, KeyboardInterrupt):
                self._exit()
            except:
                print traceback.format_exc()
            # If we reach this line, exec call failed (is it possible to reach
            # it without going through above "except" branch ?).
            print 'Error executing %r.' % (command + ' ' + ' '.join(args), )
            self._exit(-1)

    def _exit(self, status=0):
        sys.stdout = sys.stderr = open('/dev/null', 'w')
        raise KeyboardInterrupt

    def kill(self, sig=signal.SIGTERM):
        if self.pid:
            try:
                os.kill(self.pid, sig)
            except OSError:
                traceback.print_last()
        else:
            raise AlreadyStopped

    def __del__(self):
        # If we get killed, kill subprocesses aswell.
        try:
            self.kill(signal.SIGKILL)
            self.wait()
        except:
            # We can ignore all exceptions at this point, since there is no
            # garanteed way to handle them (other objects we would depend on
            # might already have been deleted).
            pass

    def wait(self, options=0):
        if self.pid == 0:
            raise AlreadyStopped
        result = os.WEXITSTATUS(os.waitpid(self.pid, options)[1])
        self.pid = 0
        if result:
            raise NodeProcessError('%r %r exited with status %r' % (
                self.command, self.arg_dict, result))
        return result

    def stop(self):
        self.kill()
        self.wait()

    def getPID(self):
        return self.pid

    def getUUID(self):
        assert self.with_uuid, 'UUID disabled on this process'
        return self.uuid

    def setUUID(self, uuid):
        """
          Note: for this change to take effect, the node must be restarted.
        """
        self.uuid = uuid
        self.arg_dict['--uuid'] = dump(uuid)

    def isAlive(self):
        try:
            os.kill(self.pid, 0)
        except OSError, (errno, msg):
            if errno == 3: # No such process
                result = False
            else:
                raise
        else:
            result = True
        return result

class NEOCluster(object):

    def __init__(self, db_list, master_node_count=1, partitions=1, replicas=0,
                 db_user='neo', db_password='neo',
                 db_super_user=DB_ADMIN, db_super_password=DB_PASSWD,
                 cleanup_on_delete=False, temp_dir=None,
                 clear_databases=True, adapter='MySQL',
                 verbose=True,
                 address_type=ADDRESS_TYPE,
        ):
        self.zodb_storage_list = []
        self.cleanup_on_delete = cleanup_on_delete
        self.verbose = verbose
        self.uuid_set = set()
        self.db_super_user = db_super_user
        self.db_super_password = db_super_password
        self.db_user = db_user
        self.db_password = db_password
        self.db_list = db_list
        self.address_type = address_type
        self.local_ip = local_ip = IP_VERSION_FORMAT_DICT[self.address_type]
        if clear_databases:
            self.setupDB()
        self.process_dict = {}
        if temp_dir is None:
            temp_dir = tempfile.mkdtemp(prefix='neo_')
            print 'Using temp directory %r.' % (temp_dir, )
        self.temp_dir = temp_dir
        self.port_allocator = PortAllocator()
        admin_port = self.port_allocator.allocate(address_type, local_ip)
        self.cluster_name = 'neo_%s' % (random.randint(0, 100), )
        master_node_list = [self.port_allocator.allocate(address_type, local_ip)
                            for i in xrange(master_node_count)]
        self.master_nodes = '/'.join('%s:%s' % (
                buildUrlFromString(self.local_ip), x, )
                for x in master_node_list)

        # create admin node
        self.__newProcess(NEO_ADMIN, {
            '--cluster': self.cluster_name,
            '--name': 'admin',
            '--bind': '%s:%d' % (buildUrlFromString(
                      self.local_ip), admin_port, ),
            '--masters': self.master_nodes,
        })
        # create master nodes
        for index, port in enumerate(master_node_list):
            self.__newProcess(NEO_MASTER, {
                '--cluster': self.cluster_name,
                '--name': 'master_%d' % index,
                '--bind': '%s:%d' % (buildUrlFromString(
                          self.local_ip), port, ),
                '--masters': self.master_nodes,
                '--replicas': replicas,
                '--partitions': partitions,
            })
        # create storage nodes
        for index, db in enumerate(db_list):
            self.__newProcess(NEO_STORAGE, {
                '--cluster': self.cluster_name,
                '--name': 'storage_%d' % index,
                '--bind': '%s:%d' % (buildUrlFromString(
                                        self.local_ip),
                                        0 ),
                '--masters': self.master_nodes,
                '--database': '%s:%s@%s' % (db_user, db_password, db),
                '--adapter': adapter,
            })
        # create neoctl
        self.neoctl = NeoCTL((self.local_ip, admin_port))

    def __newProcess(self, command, arguments):
        uuid = self.__allocateUUID()
        arguments['--uuid'] = uuid
        if self.verbose:
            arguments['--verbose'] = True
        logfile = arguments['--name']
        arguments['--logfile'] = os.path.join(self.temp_dir, '%s.log' % (logfile, ))

        self.process_dict.setdefault(command, []).append(
            NEOProcess(command, uuid, arguments))

    def __allocateUUID(self):
        uuid = os.urandom(16)
        self.uuid_set.add(uuid)
        return uuid

    def __getSuperSQLConnection(self):
        # Cleanup or bootstrap databases
        connect_arg_dict = {'user': self.db_super_user}
        password = self.db_super_password
        if password is not None:
            connect_arg_dict['passwd'] = password
        return MySQLdb.Connect(**connect_arg_dict)

    def setupDB(self):
        sql_connection = self.__getSuperSQLConnection()
        cursor = sql_connection.cursor()
        for database in self.db_list:
            cursor.execute('DROP DATABASE IF EXISTS `%s`' % (database, ))
            cursor.execute('CREATE DATABASE `%s`' % (database, ))
            cursor.execute('GRANT ALL ON `%s`.* TO "%s"@"localhost" '\
                           'IDENTIFIED BY "%s"' % (database, self.db_user,
                           self.db_password))
        cursor.close()
        sql_connection.commit()
        sql_connection.close()

    def switchTables(self, database):
        sql_connection = self.__getSuperSQLConnection()
        cursor = sql_connection.cursor()
        cursor.execute('use %s' % (database, ))
        for table in ('trans', 'obj'):
            cursor.execute('rename table %s to tmp' % (table, ))
            cursor.execute('rename table t%s to %s' % (table, table))
            cursor.execute('rename table tmp to t%s' % (table, ))
        cursor.execute('truncate table obj_short')
        sql_connection.commit()
        sql_connection.close()

    def run(self, except_storages=()):
        """ Start cluster processes except some storage nodes """
        assert len(self.process_dict)
        self.port_allocator.release()
        for process_list in self.process_dict.itervalues():
            for process in process_list:
                if process not in except_storages:
                    process.start()
        # wait for the admin node availability
        end_time = time.time() + MAX_START_TIME
        while True:
            if time.time() > end_time:
                raise AssertionError, 'Timeout when starting cluster'
            try:
                self.neoctl.getClusterState()
            except NotReadyException:
                time.sleep(0.5)
            else:
                break
        self.port_allocator.reset()

    def start(self, except_storages=()):
        """ Do a complete start of a cluster """
        self.run(except_storages=except_storages)
        neoctl = self.neoctl
        neoctl.startCluster()
        target_count = len(self.db_list) - len(except_storages)
        end_time = time.time() + MAX_START_TIME
        while True:
            storage_node_list = neoctl.getNodeList(
                node_type=NodeTypes.STORAGE)
            # wait at least number of started storages, admin node can know
            # more nodes when the cluster restart with an existing partition
            # table referencing non-running nodes
            if len(storage_node_list) >= target_count:
                break
            time.sleep(0.5)
            if time.time() > end_time:
                raise AssertionError, 'Timeout when starting cluster'
        if storage_node_list:
            self.expectClusterRunning()
            neoctl.enableStorageList([x[2] for x in storage_node_list])

    def stop(self, clients=True):
        for process_list in self.process_dict.itervalues():
            for process in process_list:
                try:
                    process.kill(signal.SIGKILL)
                    process.wait()
                except AlreadyStopped:
                    pass
        if clients:
            for zodb_storage in self.zodb_storage_list:
                zodb_storage.close()
            self.zodb_storage_list = []
        time.sleep(0.5)

    def getNEOCTL(self):
        return self.neoctl

    def getZODBStorage(self,connector = SOCKET_CONNECTORS_DICT[ADDRESS_TYPE], **kw):
        master_nodes = self.master_nodes.replace('/', ' ')
        result = Storage(
            master_nodes=master_nodes,
            name=self.cluster_name,
            connector=connector,
            logfile=os.path.join(self.temp_dir, 'client.log'),
            verbose=self.verbose,
            **kw
        )
        self.zodb_storage_list.append(result)
        return result

    def getZODBConnection(self, **kw):
        """ Return a tuple with the database and a connection """
        db = ZODB.DB(storage=self.getZODBStorage(**kw))
        return (db, db.open())

    def getSQLConnection(self, db, autocommit=False):
        assert db in self.db_list
        conn = MySQLdb.Connect(user=self.db_user, passwd=self.db_password,
                               db=db)
        conn.autocommit(autocommit)
        return conn

    def _getProcessList(self, type):
        return self.process_dict.get(type)

    def getMasterProcessList(self):
        return self._getProcessList(NEO_MASTER)

    def getStorageProcessList(self):
        return self._getProcessList(NEO_STORAGE)

    def getAdminProcessList(self):
        return self._getProcessList(NEO_ADMIN)

    def _killMaster(self, primary=False, all=False):
        killed_uuid_list = []
        primary_uuid = self.neoctl.getPrimary()
        for master in self.getMasterProcessList():
            master_uuid = master.getUUID()
            is_primary = master_uuid == primary_uuid
            if primary and is_primary or not (primary or is_primary):
                killed_uuid_list.append(master_uuid)
                master.kill()
                master.wait()
                if not all:
                    break
        return killed_uuid_list

    def killPrimary(self):
        return self._killMaster(primary=True)

    def killSecondaryMaster(self, all=False):
        return self._killMaster(primary=False, all=all)

    def killMasters(self):
        secondary_list = self.killSecondaryMaster(all=True)
        primary_list = self.killPrimary()
        return secondary_list + primary_list

    def killStorage(self, all=False):
        killed_uuid_list = []
        for storage in self.getStorageProcessList():
            killed_uuid_list.append(storage.getUUID())
            storage.kill()
            storage.wait()
            if not all:
                break
        return killed_uuid_list

    def __getNodeList(self, node_type, state=None):
        return [x for x in self.neoctl.getNodeList(node_type)
                if state is None or x[3] == state]

    def getMasterList(self, state=None):
        return self.__getNodeList(NodeTypes.MASTER, state)

    def getStorageList(self, state=None):
        return self.__getNodeList(NodeTypes.STORAGE, state)

    def getClientlist(self, state=None):
        return self.__getNodeList(NodeTypes.CLIENT, state)

    def __getNodeState(self, node_type, uuid):
        node_list = self.__getNodeList(node_type)
        for node_type, address, node_uuid, state in node_list:
            if node_uuid == uuid:
                break
        else:
            state = None
        return state

    def getMasterNodeState(self, uuid):
        return self.__getNodeState(NodeTypes.MASTER, uuid)

    def getPrimary(self):
        try:
            current_try = self.neoctl.getPrimary()
        except NotReadyException:
            current_try = None
        return current_try

    def expectCondition(self, condition, timeout=0, delay=1, on_fail=None):
        end = time.time() + timeout + DELAY_SAFETY_MARGIN
        opaque = None
        opaque_history = []
        while time.time() < end:
            reached, opaque = condition(opaque)
            if reached:
                break
            else:
                opaque_history.append(opaque)
                time.sleep(delay)
        else:
            if on_fail is not None:
                on_fail(opaque_history)
            raise AssertionError, 'Timeout while expecting condition. ' \
                                'History: %s' % (opaque_history, )

    def expectAllMasters(self, node_count, state=None, timeout=0, delay=1):
        def callback(last_try):
            current_try = len(self.getMasterList(state=state))
            if last_try is not None and current_try < last_try:
                raise AssertionError, 'Regression: %s became %s' % \
                    (last_try, current_try)
            return (current_try == node_count, current_try)
        self.expectCondition(callback, timeout, delay)

    def __expectNodeState(self, node_type, uuid, state, timeout=0, delay=1):
        if not isinstance(state, (tuple, list)):
            state = (state, )
        def callback(last_try):
            current_try = self.__getNodeState(node_type, uuid)
            return current_try in state, current_try
        self.expectCondition(callback, timeout, delay)

    def expectMasterState(self, uuid, state, timeout=0, delay=1):
        self.__expectNodeState(NodeTypes.MASTER, uuid, state, timeout,
                delay)

    def expectStorageState(self, uuid, state, timeout=0, delay=1):
        self.__expectNodeState(NodeTypes.STORAGE, uuid, state,
                timeout, delay)

    def expectRunning(self, process, timeout=0, delay=1):
        self.expectStorageState(process.getUUID(), NodeStates.RUNNING, timeout,
                delay)

    def expectPending(self, process, timeout=0, delay=1):
        self.expectStorageState(process.getUUID(), NodeStates.PENDING, timeout,
                delay)

    def expectUnknown(self, process, timeout=0, delay=1):
        self.expectStorageState(process.getUUID(), NodeStates.UNKNOWN, timeout,
                delay)

    def expectUnavailable(self, process, timeout=0, delay=1):
        self.expectStorageState(process.getUUID(),
                NodeStates.TEMPORARILY_DOWN, timeout, delay)

    def expectPrimary(self, uuid=None, timeout=0, delay=1):
        def callback(last_try):
            current_try = self.getPrimary()
            if None not in (uuid, current_try) and uuid != current_try:
                raise AssertionError, 'An unexpected primary arised: %r, ' \
                    'expected %r' % (dump(current_try), dump(uuid))
            return uuid is None or uuid == current_try, current_try
        self.expectCondition(callback, timeout, delay)

    def expectOudatedCells(self, number, timeout=0, delay=1):
        def callback(last_try):
            row_list = self.neoctl.getPartitionRowList()[1]
            number_of_oudated = 0
            for row in row_list:
                for cell in row[1]:
                    if cell[1] == CellStates.OUT_OF_DATE:
                        number_of_oudated += 1
            return number_of_oudated == number, number_of_oudated
        self.expectCondition(callback, timeout, delay)

    def expectAssignedCells(self, process, number, timeout=0, delay=1):
        def callback(last_try):
            row_list = self.neoctl.getPartitionRowList()[1]
            assigned_cells_number = 0
            for row in row_list:
                for cell in row[1]:
                    if cell[0] == process.getUUID():
                        assigned_cells_number += 1
            return assigned_cells_number == number, assigned_cells_number
        self.expectCondition(callback, timeout, delay)

    def expectClusterState(self, state, timeout=0, delay=1):
        def callback(last_try):
            current_try = self.neoctl.getClusterState()
            return current_try == state, current_try
        self.expectCondition(callback, timeout, delay)

    def expectClusterRecovering(self, timeout=0, delay=1):
        self.expectClusterState(ClusterStates.RECOVERING, timeout, delay)

    def expectClusterVerifying(self, timeout=0, delay=1):
        self.expectClusterState(ClusterStates.VERIFYING, timeout, delay)

    def expectClusterRunning(self, timeout=0, delay=1):
        self.expectClusterState(ClusterStates.RUNNING, timeout, delay)

    def expectAlive(self, process, timeout=0, delay=1):
        def callback(last_try):
            current_try = process.isAlive()
            return current_try, current_try
        self.expectCondition(callback, timeout, delay)

    def expectStorageNotKnown(self, process, timeout=0, delay=1):
        # /!\ Not Known != Unknown
        process_uuid = process.getUUID()
        def expected_storage_not_known(last_try):
            for storage in self.getStorageList():
                if storage[2] == process_uuid:
                    return False, storage
            return True, None
        self.expectCondition(expected_storage_not_known, timeout, delay)

    def __del__(self):
        if self.cleanup_on_delete:
            os.removedirs(self.temp_dir)


class NEOFunctionalTest(NeoTestBase):

    def getTempDirectory(self):
        # build the full path based on test case and current test method
        temp_dir = os.path.join(getTempDirectory(), self.id())
        # build the path if needed
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        return temp_dir

    def runWithTimeout(self, timeout, method, args=(), kwargs=None):
        if kwargs is None:
            kwargs = {}
        exc_list = []
        def excWrapper(*args, **kw):
            try:
                method(*args, **kw)
            except:
                exc_list.append(sys.exc_info())
        thread = threading.Thread(None, excWrapper, args=args, kwargs=kwargs)
        thread.setDaemon(True)
        thread.start()
        thread.join(timeout)
        self.assertFalse(thread.isAlive(), 'Run timeout')
        if exc_list:
            assert len(exc_list) == 1, exc_list
            exc = exc_list[0]
            raise exc[0], exc[1], exc[2]

