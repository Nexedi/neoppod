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
import signal
import random
import MySQLdb
import tempfile
import traceback

from neo import setupLog
from neo import logging
from neo.client.Storage import Storage

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

class AlreadyRunning(Exception):
    pass

class AlreadyStopped(Exception):
    pass

class NEOProcess:
    pid = 0

    def __init__(self, command, *args):
        self.command = command
        self.args = args

    def start(self):
        # Prevent starting when already forked and wait wasn't called.
        if self.pid != 0:
            raise AlreadyRunning, 'Already running with PID %r' % (self.pid, )
        command = self.command
        args = self.args
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

class NEOCluster(object):
    def __init__(self, db_list, master_node_count=1,
                 partitions=1, replicas=0, port_base=10000,
                 db_user='neo', db_password='neo',
                 db_super_user='root', db_super_password=None,
                 cleanup_on_delete=False):
        self.cleanup_on_delete = cleanup_on_delete
        self.db_super_user = db_super_user
        self.db_super_password = db_super_password
        self.db_user = db_user
        self.db_password = db_password
        self.db_list = db_list
        self.process_list = []
        self.last_port = port_base
        self.temp_dir = temp_dir = tempfile.mkdtemp(prefix='neo_')
        print 'Using temp directory %r.' % (temp_dir, )
        config_file_path = os.path.join(temp_dir, 'neo.conf')
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
        self.__newProcess(NEO_ADMIN, '-vc', config_file_path, '-s', 'admin',
                          '-l', os.path.join(temp_dir, 'admin.log'))
        for config_id, port in master_node_dict.iteritems():
            config_file.write(NEO_CONFIG_MASTER % {
                'id': config_id,
                'port': port,
            })
            self.__newProcess(NEO_MASTER, '-vc', config_file_path, '-s',
                config_id, '-l',
                os.path.join(temp_dir, '%s.log' % (config_id)))
        for storage, db in enumerate(db_list):
            config_id = NEO_STORAGE_ID % (storage, )
            config_file.write(NEO_CONFIG_STORAGE % {
                'id': config_id,
                'db': db,
                'port': self.__allocatePort(),
            })
            self.__newProcess(NEO_STORAGE, '-vc', config_file_path, '-s',
                config_id, '-l',
                os.path.join(temp_dir, '%s.log' % (config_id)))
        config_file.close()
        self.neoctl = NeoCTL('127.0.0.1', neo_admin_port,
                             'SocketConnector')

    def __newProcess(self, command, *args):
        self.process_list.append(NEOProcess(command, *args))

    def __allocatePort(self):
        port = self.last_port
        self.last_port += 1
        return port

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

    def start(self):
        neoctl = self.neoctl
        assert len(self.process_list)
        for process in self.process_list:
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
        target_count = len(self.db_list)
        while True:
            storage_node_list = neoctl.getNodeList(
                node_type=protocol.STORAGE_NODE_TYPE)
            if len(storage_node_list) == target_count:
                break
            time.sleep(0.5)
        neoctl.enableStorageList([x[2] for x in storage_node_list])

    def stop(self):
        for process in self.process_list:
            try:
                process.kill()
                process.wait()
            except AlreadyStopped:
                pass

    def getNEOCTL(self):
        return self.neoctl

    def getStorage(self):
        return Storage(
            master_nodes=self.master_nodes,
            name=self.cluster_name,
            connector='SocketConnector')

    def __del__(self):
        if self.cleanup_on_delete:
            os.removedirs(self.temp_dir)

