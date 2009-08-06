#
# Copyright (C) 2009  Nexedi SA
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

from neo.neoctl.neoctl import NeoCTL, NotReadyException
from neo import protocol
import os
import sys
import time
import ZODB
import signal
import random
import MySQLdb
import tempfile
import traceback

from neo.client.Storage import Storage
from neo.tests import getNewUUID
from neo.util import dump

NEO_CONFIG_HEADER = """
[DEFAULT]
master_nodes: %(master_nodes)s
replicas: %(replicas)s
partitions: %(partitions)s
name: %(name)s
user: %(user)s
password: %(password)s
connector: SocketConnector

[admin]
server: 127.0.0.1:%(port)s
"""

NEO_CONFIG_MASTER = """
[%(id)s]
server: 127.0.0.1:%(port)s
"""

NEO_CONFIG_STORAGE = """
[%(id)s]
database: %(db)s
server: 127.0.0.1:%(port)s
"""

NEO_MASTER_ID = 'master%s'
NEO_STORAGE_ID = 'storage%s'

NEO_MASTER = 'neomaster'
NEO_STORAGE = 'neostorage'
NEO_ADMIN = 'neoadmin'

DELAY_SAFETY_MARGIN = 10

class AlreadyRunning(Exception):
    pass

class AlreadyStopped(Exception):
    pass

class NEOProcess:
    pid = 0

    def __init__(self, command, uuid, port, arg_dict):
        self.command = command
        self.arg_dict = arg_dict
        self.setUUID(uuid)
        self.port = port

    def start(self):
        # Prevent starting when already forked and wait wasn't called.
        if self.pid != 0:
            raise AlreadyRunning, 'Already running with PID %r' % (self.pid, )
        command = self.command
        args = []
        for arg, param in self.arg_dict.iteritems():
            args.append(arg)
            if param is not None:
                args.append(param)
        self.pid = os.fork()
        if self.pid == 0:
            # Child
            try:
                os.execlp(command, command, *args)
            except:
                print traceback.format_exc()
            # If we reach this line, exec call failed (is it possible to reach
            # it without going through above "except" branch ?).
            print 'Error executing %r.' % (command + ' ' + ' '.join(args), )
            # KeyboardInterrupt is not intercepted by test runner (it is still
            # above us in the stack), and we do want to exit.
            # To avoid polluting test foreground output with induced
            # traceback, replace stdout & stderr.
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
        return result

    def stop(self):
        self.kill()
        self.wait()

    def getUUID(self):
        return self.uuid

    def setUUID(self, uuid):
        """
          Note: for this change to take effect, the node must be restarted.
        """
        self.uuid = uuid
        self.arg_dict['-u'] = dump(uuid)

    def getPort(self):
        return self.port

class NEOCluster(object):

    def __init__(self, db_list, master_node_count=1,
                 partitions=1, replicas=0, port_base=10000,
                 db_user='neo', db_password='neo',
                 db_super_user='root', db_super_password=None,
                 cleanup_on_delete=False):
        self.cleanup_on_delete = cleanup_on_delete
        self.uuid_set = set()
        self.db_super_user = db_super_user
        self.db_super_password = db_super_password
        self.db_user = db_user
        self.db_password = db_password
        self.db_list = db_list
        self.process_dict = {}
        self.last_port = port_base
        self.temp_dir = temp_dir = tempfile.mkdtemp(prefix='neo_')
        print 'Using temp directory %r.' % (temp_dir, )
        self.config_file_path = config_file_path = os.path.join(temp_dir, 'neo.conf')
        config_file = open(config_file_path, 'w')
        neo_admin_port = self.__allocatePort()
        self.cluster_name = cluster_name = 'neo_%s' % (random.randint(0, 100), )
        master_node_dict = {}
        for master in xrange(master_node_count):
            master_node_dict[NEO_MASTER_ID % (master, )] = \
                self.__allocatePort()
        self.master_nodes = master_nodes = ' '.join('127.0.0.1:%s' % 
            (x, ) for x in master_node_dict.itervalues())
        config_file.write(NEO_CONFIG_HEADER % {
            'master_nodes': master_nodes,
            'replicas': replicas,
            'partitions': partitions,
            'name': cluster_name,
            'user': db_user,
            'password': db_password,
            'port': neo_admin_port,
        })
        self.__newProcess(NEO_ADMIN, 'admin', neo_admin_port)
        for config_id, port in master_node_dict.iteritems():
            config_file.write(NEO_CONFIG_MASTER % {
                'id': config_id,
                'port': port,
            })
            self.__newProcess(NEO_MASTER, config_id, port)
        for storage, db in enumerate(db_list):
            config_id = NEO_STORAGE_ID % (storage, )
            port = self.__allocatePort()
            config_file.write(NEO_CONFIG_STORAGE % {
                'id': config_id,
                'db': db,
                'port': port,
            })
            self.__newProcess(NEO_STORAGE, config_id, port)
        config_file.close()
        self.neoctl = NeoCTL('127.0.0.1', neo_admin_port,
                             'SocketConnector')

    def __newProcess(self, command, section, port):
        uuid = self.__allocateUUID()
        self.process_dict.setdefault(command, []).append(
            NEOProcess(command, uuid, port, {
                '-v': None,
                '-c': self.config_file_path,
                '-s': section,
                '-l': os.path.join(self.temp_dir, '%s.log' % (section, ))
            }))

    def __allocatePort(self):
        port = self.last_port
        self.last_port += 1
        return port

    def __allocateUUID(self):
        uuid_set = self.uuid_set
        uuid = None
        while uuid is None or uuid in uuid_set:
            uuid = getNewUUID()
        uuid_set.add(uuid)
        return uuid

    def setupDB(self):
        # Cleanup or bootstrap databases
        connect_arg_dict = {'user': self.db_super_user}
        password = self.db_super_password
        if password is not None:
            connect_arg_dict['passwd'] = password
        sql_connection = MySQLdb.Connect(**connect_arg_dict)
        cursor = sql_connection.cursor()
        for database in self.db_list:
            cursor.execute('DROP DATABASE IF EXISTS `%s`' % (database, ))
            cursor.execute('CREATE DATABASE `%s`' % (database, ))
            cursor.execute('GRANT ALL ON `%s`.* TO "%s"@"localhost" '\
                           'IDENTIFIED BY "%s"' % (database, self.db_user,
                           self.db_password))
        cursor.close()
        sql_connection.close()

    def start(self, except_storages=[]):
        neoctl = self.neoctl
        assert len(self.process_dict)
        for process_list in self.process_dict.itervalues():
            for process in process_list:
                if process not in except_storages:
                    process.start()
        # Try to put cluster in running state. This will succeed as soon as
        # admin node could connect to the primary master node.
        while True:
            try:
                neoctl.startCluster()
            except NotReadyException:
                time.sleep(0.5)
            else:
                break
        target_count = len(self.db_list) - len(except_storages)
        while True:
            storage_node_list = neoctl.getNodeList(
                node_type=protocol.STORAGE_NODE_TYPE)
            if len(storage_node_list) == target_count:
                break
            time.sleep(0.5)
        neoctl.enableStorageList([x[2] for x in storage_node_list])

    def stop(self):
        for process_list in self.process_dict.itervalues():
            for process in process_list:
                try:
                    process.kill()
                    process.wait()
                except AlreadyStopped:
                    pass

    def getNEOCTL(self):
        return self.neoctl

    def getZODBStorage(self):
        return Storage(
            master_nodes=self.master_nodes,
            name=self.cluster_name,
            connector='SocketConnector')

    def getZODBConnection(self):
        """ Return a tuple with the database and a connection """
        db = ZODB.DB(storage=self.getZODBStorage())
        return (db, db.open())

    def getSQLConnection(self, db):
        assert db in self.db_list
        return MySQLdb.Connect(user=self.db_user, passwd=self.db_password,
                               db=db)

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
        primary_uuid = self.neoctl.getPrimaryMaster()
        for master in self.getMasterProcessList():
            master_uuid = master.getUUID()
            is_primary = master_uuid == primary_uuid
            if primary and is_primary or \
               not (primary or is_primary):
                 killed_uuid_list.append(master_uuid)
                 master.kill()
                 master.wait()
                 if not all:
                     break
        return killed_uuid_list

    def killPrimaryMaster(self):
        return self._killMaster(primary=True)

    def killSecondaryMaster(self, all=False):
        return self._killMaster(primary=False, all=all)

    def killMasters(self):
        secondary_list = self.killSecondaryMaster(all=True)
        primary_list = self.killPrimaryMaster()
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

    def getMasterNodeList(self, state=None):
        return self.__getNodeList(protocol.MASTER_NODE_TYPE, state)

    def getStorageNodeList(self, state=None):
        return self.__getNodeList(protocol.STORAGE_NODE_TYPE, state)

    def __getNodeState(self, node_type, uuid):
        node_list = self.__getNodeList(node_type)
        for node_type, address, node_uuid, state in node_list:
            if node_uuid == uuid:
                break
        else:
            state = None
        return state

    def getMasterNodeState(self, uuid):
        return self.__getNodeState(protocol.MASTER_NODE_TYPE, uuid)

    def getPrimaryMaster(self):
        try:
            current_try = self.neoctl.getPrimaryMaster()
        except NotReadyException:
            current_try = None
        return current_try

    def expectCondition(self, condition, timeout=0, delay=1):
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
          raise AssertionError, 'Timeout while expecting condition. ' \
                                'History: %s' % (opaque_history, )

    def expectAllMasters(self, node_count, state=None, timeout=0, delay=1):
        def callback(last_try):
            current_try = len(self.getMasterNodeList(state=state))
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
        self.__expectNodeState(protocol.MASTER_NODE_TYPE, uuid, state, timeout,
                delay)

    def expectStorageState(self, uuid, state, timeout=0, delay=1):
        self.__expectNodeState(protocol.STORAGE_NODE_TYPE, uuid, state, 
                timeout,delay)

    def expectPrimaryMaster(self, uuid=None, timeout=0, delay=1):
        def callback(last_try):
            current_try = self.getPrimaryMaster()
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
                    if cell[1] == protocol.OUT_OF_DATE_STATE:
                        number_of_oudated += 1
            return number_of_oudated == number, number_of_oudated
        self.expectCondition(callback, timeout, delay)

    def expectClusterState(self, state, timeout=0, delay=1):
        def callback(last_try):
            current_try = self.neoctl.getClusterState()
            return current_try == state, current_try
        self.expectCondition(callback, timeout, delay)

    def expectClusterRecovering(self, timeout=0, delay=1):
        self.expectClusterState(protocol.RECOVERING)

    def expectClusterVeryfing(self, timeout=0, delay=1):
        self.expectClusterState(protocol.VERIFYING)

    def expectClusterRunning(self, timeout=0, delay=1):
        self.expectClusterState(protocol.RUNNING)

    def __del__(self):
        if self.cleanup_on_delete:
            os.removedirs(self.temp_dir)

