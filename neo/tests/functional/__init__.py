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
import MySQLdb
import tempfile
import traceback

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
            print 'Error executing %r.' % (command + ' ' + ' '.join(args), )
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
NEO_ADMIN = 'neoadmin'
NEO_PORT_BASE = 10010
NEO_CLUSTER_NAME = 'test'
NEO_MASTER_PORT_1 = NEO_PORT_BASE
NEO_MASTER_PORT_2 = NEO_MASTER_PORT_1 + 1
NEO_MASTER_PORT_3 = NEO_MASTER_PORT_2 + 1
NEO_STORAGE_PORT_1 = NEO_MASTER_PORT_3 + 1
NEO_STORAGE_PORT_2 = NEO_STORAGE_PORT_1 + 1
NEO_STORAGE_PORT_3 = NEO_STORAGE_PORT_2 + 1
NEO_STORAGE_PORT_4 = NEO_STORAGE_PORT_3 + 1
NEO_ADMIN_PORT = NEO_STORAGE_PORT_4 + 1
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

# The admin node.
[admin]
server: 127.0.0.1:%(admin_port)s

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
    'admin_port': NEO_ADMIN_PORT,
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
a_log = os.path.join(temp_dir, 'a.log')


from neo import setupLog
client_log = os.path.join(temp_dir, 'c.log')
setupLog('CLIENT', filename=client_log, verbose=True)
from neo import logging
from neo.client.Storage import Storage

neoctl = NeoCTL('127.0.0.1', NEO_ADMIN_PORT, 'SocketConnector')

def startNeo():
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
    NEOProcess(NEO_ADMIN, '-vc', config_file_path, '-s', 'admin', '-l', a_log)
    # Try to put cluster in running state. This will succeed as soon as
    # admin node could connect to the primary master node.
    while True:
        try:
            neoctl.startCluster()
        except NotReadyException:
            time.sleep(0.5)
        else:
            break
    while True:
        storage_node_list = neoctl.getNodeList(
            node_type=protocol.STORAGE_NODE_TYPE)
        if len(storage_node_list) == 4:
            break
        time.sleep(0.5)
    neoctl.enableStorageList([x[2] for x in storage_node_list])

def getNeoStorage():
    return Storage(
        master_nodes=NEO_MASTER_NODES,
        name=NEO_CLUSTER_NAME,
        connector='SocketConnector')

