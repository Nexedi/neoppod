#
# Copyright (C) 2009-2019  Nexedi SA
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

from __future__ import print_function
import errno
import os
import sys
import time
import socket
import signal
import random
import sqlite3
import unittest
import tempfile
import traceback
import threading
from collections import OrderedDict
from neo import *
import psutil
import ZODB

import neo.scripts
from neo.neoctl.neoctl import NeoCTL, NotReadyException
from neo.lib import logging
from neo.lib.connector import SocketConnector
from neo.lib.protocol import ClusterStates, NodeTypes, CellStates, NodeStates, \
    UUID_NAMESPACES
from neo.lib.util import dump, setproctitle
from .. import (ADDRESS_TYPE, IP_VERSION_FORMAT_DICT, SSL,
    buildUrlFromString, cluster, getTempDirectory, setupMySQL,
    ImporterConfigParser, NeoTestBase, Patch)
from neo.client.Storage import Storage
from neo.storage.database import manager, buildDatabaseManager

try:
    coverage = sys.modules['neo.scripts.runner'].coverage
except (AttributeError, KeyError):
    coverage = None

command_dict = {
    NodeTypes.MASTER: 'neomaster',
    NodeTypes.STORAGE: 'neostorage',
    NodeTypes.ADMIN: 'neoadmin',
}

DELAY_SAFETY_MARGIN = 10
MAX_START_TIME = 30

NEOMASTER_PYPY = os.getenv('NEOMASTER_PYPY')
if NEOMASTER_PYPY:
    import neo, msgpack
    PYPY_TEMPLATE = """\
import os, signal, sys
def sigusr2(*_):
    os.close(%r)
    os.kill(os.getpid(), signal.SIGSTOP)
signal.signal(signal.SIGUSR2, sigusr2)
os.close(%r)
os.write(%r, '\\0')
sys.path.append({!r}); import msgpack; del sys.path[-1]
sys.path.insert(0, {!r}); import neo; del sys.path[0]
from neo.lib import logging
logging.default_root_handler.handle = lambda record: None
logging.backlog(%s, %s)
from neo.scripts.%s import main
main()
""".format(os.path.dirname(*msgpack.__path__), os.path.dirname(*neo.__path__))

class NodeProcessError(Exception):
    pass

class AlreadyRunning(Exception):
    pass

class AlreadyStopped(Exception):
    pass

class NotFound(Exception):
    pass

class PortAllocator(object):

    def __init__(self):
        self.socket_list = []
        self.tried_port_set = set()

    def allocate(self, address_type, local_ip):
        min_port = n = 16384
        max_port = min_port + n
        tried_port_set = self.tried_port_set
        while True:
            s = socket.socket(address_type, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Find an unreserved port.
            while True:
                # Do not let the system choose the port to avoid conflicts
                # with other software. IOW, use a range different than:
                # - /proc/sys/net/ipv4/ip_local_port_range on Linux
                # - what IANA recommends (49152 to 65535)
                port = random.randrange(min_port, max_port)
                if port not in tried_port_set:
                    tried_port_set.add(port)
                    try:
                        s.bind((local_ip, port))
                        break
                    except socket.error as e:
                        if e.errno != errno.EADDRINUSE:
                            raise
                elif len(tried_port_set) >= n:
                    raise RuntimeError("No free port")
            # Reserve port.
            try:
                s.listen(1)
                self.socket_list.append(s)
                return port
            except socket.error as e:
                if e.errno != errno.EADDRINUSE:
                    raise

    def release(self):
        for s in self.socket_list:
            s.close()
        self.__init__()

    __del__ = release


class Process(object):

    _coverage_fd = None
    _coverage_prefix = None
    _coverage_index = 0
    on_fork = [logging.resetNids]
    pid = 0

    def __init__(self, command, *args, **kw):
        self.command = command
        self.args = args
        self.arg_dict = kw

    def _args(self):
        args = []
        for arg, param in six.iteritems(self.arg_dict):
            args.append('--' + arg)
            if param is not None:
                args.append(str(param))
        args += self.args
        return args

    def start(self):
        # Prevent starting when already forked and wait wasn't called.
        if self.pid != 0:
            raise AlreadyRunning('Already running with PID %r' % self.pid)
        command = self.command
        args = self._args()
        global coverage
        if coverage:
            cls = self.__class__
            cls._coverage_index += 1
            if not cls._coverage_prefix:
                cls._coverage_prefix = os.path.join(
                    getTempDirectory(), 'coverage-')
            coverage_data_path = cls._coverage_prefix + str(cls._coverage_index)
        self._coverage_fd, w = os.pipe()
        def save_coverage(*args):
            if coverage:
                coverage.stop()
                coverage.save()
            if args:
                os.close(w)
                os.kill(os.getpid(), signal.SIGSTOP)
        self.pid = os.fork()
        if self.pid:
            # Wait that the signal to kill the child is set up.
            os.close(w)
            os.read(self._coverage_fd, 1)
            if coverage:
                coverage.neotestrunner.append(coverage_data_path)
        else:
            # Child
            try:
                signal.signal(signal.SIGTERM, lambda *args: sys.exit())
                if coverage:
                    coverage.stop()
                    from coverage import Coverage
                    coverage = Coverage(coverage_data_path)
                    coverage.start()
                elif NEOMASTER_PYPY and command == 'neomaster':
                    os.execlp(NEOMASTER_PYPY, NEOMASTER_PYPY, '-c',
                        PYPY_TEMPLATE % (
                            w, self._coverage_fd, w,
                            logging._max_size, logging._max_packet,
                            command),
                        *args)
                signal.signal(signal.SIGUSR2, save_coverage)
                os.close(self._coverage_fd)
                os.write(w, b'\0')
                sys.argv = [command] + args
                setproctitle(command)
                for on_fork in self.on_fork:
                    on_fork()
                self.run()
                status = 0
            except SystemExit as e:
                status = e.code
                if status is None:
                    status = 0
            except KeyboardInterrupt:
                status = 1
            except:
                status = -1
                traceback.print_exc()
            finally:
                # prevent child from killing anything (cf __del__), or
                # running any other cleanup code normally done by the parent
                try:
                    save_coverage()
                    os._exit(status)
                except:
                    print(status, file=sys.stderr)
                finally:
                    os._exit(1)
        logging.info('pid %u: %s %s',
            self.pid, command, ' '.join(map(repr, args)))

    def run(self):
        raise NotImplementedError

    def child_coverage(self):
        r = self._coverage_fd
        if r is not None:
            try:
                os.read(r, 1)
            finally:
                os.close(r)
                del self._coverage_fd

    def kill(self, sig=signal.SIGTERM):
        pid = self.pid
        if pid:
            logging.info('kill pid %r sig %r', pid, sig)
            try:
                pdb.kill(pid, sig)
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
            # guaranteed way to handle them (other objects we would depend on
            # might already have been deleted).
            pass
        assert self._coverage_fd is None, self._coverage_fd

    def wait(self):
        if self.pid == 0:
            raise AlreadyStopped
        result = [] # nonlocal
        def test():
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid == self.pid:
                result.append(status)
                return True
        if pdb.wait(test, DELAY_SAFETY_MARGIN):
            result = os.WEXITSTATUS(*result)
        else:
            self.kill(signal.SIGKILL)
            os.waitpid(self.pid, 0)
            result = -1
        self.pid = 0
        self.child_coverage()
        if result:
            # BUG: Unfortunately, we have random failures because there's no
            #      easy way to have atomic sections (e.g. when starting a
            #      thread) or protect finally clauses (see PEP 419) and the
            #      child process may not exit properly.
            raise NodeProcessError('%r %r %r %s' % (
                self.command, self.args, self.arg_dict,
                "had to be SIGKILL'ed" if result < 0 else
                "exited with status %r" % result))
        return result

    def stop(self):
        self.kill()
        self.wait()

    def isAlive(self):
        try:
            return psutil.Process(self.pid).status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False

class NEOProcess(Process):

    def __init__(self, command, *args, **kw):
        try:
            __import__('neo.scripts.' + command, level=0)
        except ImportError:
            raise NotFound(command + ' not found')
        self.setUUID(kw.pop('uuid', None))
        super(NEOProcess, self).__init__(command, *args, **kw)

    def _args(self):
        args = super(NEOProcess, self)._args()
        if self.uuid:
            args[:0] = '--nid', str(self.uuid)
        return args

    def run(self):
        getattr(neo.scripts,  self.command).main()

    def getUUID(self):
        return self.uuid

    def setUUID(self, uuid):
        """
          Note: for this change to take effect, the node must be restarted.
        """
        self.uuid = uuid

    @property
    def logfile(self):
        return self.arg_dict['logfile']

class NEOCluster(object):

    SSL = None

    def __init__(self, db_list, master_count=1, partitions=1, replicas=0,
                 name=None,
                 cleanup_on_delete=False, temp_dir=None, clear_databases=True,
                 adapter=os.getenv('NEO_TESTS_ADAPTER'),
                 address_type=ADDRESS_TYPE, bind_ip=None, logger=True,
                 importer=None, storage_kw={}, autostart=False):
        if not adapter:
            adapter = 'MySQL'
        self.adapter = adapter
        self.zodb_storage_list = []
        self.cleanup_on_delete = cleanup_on_delete
        self.uuid_dict = {}
        self.db_list = db_list
        if temp_dir is None:
            temp_dir = tempfile.mkdtemp(prefix='neo_')
            print('Using temp directory', temp_dir)
        if adapter == 'MySQL':
            self.db_template = setupMySQL(db_list, clear_databases)
        elif adapter == 'SQLite':
            self.db_template = (lambda t: lambda db:
                ':memory:' if db is None else db if os.sep in db else t % db
                )(os.path.join(temp_dir, '%s.sqlite'))
            if clear_databases:
                for db in self.db_list:
                    if db is None:
                        continue
                    db = self.db_template(db)
                    try:
                        os.remove(db)
                    except OSError as e:
                        if e.errno != errno.ENOENT:
                            raise
                    else:
                        logging.debug('%r deleted', db)
        else:
            assert False, adapter
        self.address_type = address_type
        self.local_ip = local_ip = bind_ip or \
            IP_VERSION_FORMAT_DICT[self.address_type]
        if importer:
            cfg = ImporterConfigParser(adapter, **importer)
            cfg.set("neo", "database", self.db_template(*db_list))
            importer_conf = os.path.join(temp_dir, 'importer.cfg')
            with open(importer_conf, 'w') as f:
                cfg.write(f)
            adapter = "Importer"
            self.db_template = str
            db_list = importer_conf,
        # To speed up start-up, start master first and admin last, which
        # reduces the probability of ECONNREFUSED. For the remaining
        # connection retries, we also patch CONNECT_LIMIT in NEOFunctionalTest.
        self.process_dict = OrderedDict()
        self.temp_dir = temp_dir
        self.port_allocator = PortAllocator()
        self.admin_port = self.port_allocator.allocate(address_type, local_ip)
        self.cluster_name = name or 'neo_%s' % random.randint(0, 100)
        master_node_list = [self.port_allocator.allocate(address_type, local_ip)
                            for i in range(master_count)]
        self.master_nodes = ' '.join('%s:%s' % (
                buildUrlFromString(self.local_ip), x, )
                for x in master_node_list)
        # create master nodes
        kw = dict(partitions=partitions, replicas=replicas)
        if autostart:
            kw['autostart'] = len(db_list)
        for i, port in enumerate(master_node_list):
            self._newProcess(NodeTypes.MASTER, logger and 'master_%u' % i,
                             port, **kw)
        # create storage nodes
        for i, db in enumerate(db_list):
            self._newProcess(NodeTypes.STORAGE, logger and 'storage_%u' % i,
                             0, adapter=adapter, database=self.db_template(db),
                             **storage_kw)
        # create admin node
        self._newProcess(NodeTypes.ADMIN, logger and 'admin', self.admin_port)

    def _newProcess(self, node_type, logfile=None, port=None, **kw):
        self.uuid_dict[node_type] = uuid = 1 + self.uuid_dict.get(node_type, 0)
        uuid += UUID_NAMESPACES[node_type] << 24
        kw['cluster'] = self.cluster_name
        kw['masters'] = self.master_nodes
        if logfile:
            kw['logfile'] = os.path.join(self.temp_dir, logfile + '.log')
        if port is not None:
            kw['bind'] = '%s:%u' % (buildUrlFromString(self.local_ip), port)
        if self.SSL:
            kw['ca'], kw['cert'], kw['key'] = self.SSL
        self.process_dict.setdefault(node_type, []).append(
            NEOProcess(command_dict[node_type], uuid=uuid, **kw))

    def run(self, except_storages=()):
        """ Start cluster processes except some storage nodes """
        assert len(self.process_dict)
        self.port_allocator.release()
        for process_list in six.itervalues(self.process_dict):
            for process in process_list:
                if process not in except_storages:
                    process.start()
        self.neoctl = NeoCTL((str2bytes(self.local_ip), self.admin_port),
                             ssl_credentials=self.SSL)
        # wait for the admin node availability
        def test():
            try:
                self.neoctl.getClusterState()
            except NotReadyException:
                return False
            return True
        if not pdb.wait(test, MAX_START_TIME):
            raise AssertionError('Timeout when starting cluster')

    def start(self, except_storages=()):
        """ Do a complete start of a cluster """
        self.run(except_storages=except_storages)
        neoctl = self.neoctl
        target = [len(self.db_list) - len(except_storages)]
        def test():
            try:
                state = neoctl.getClusterState()
                if state == ClusterStates.RUNNING:
                    return True
                if state == ClusterStates.RECOVERING and target[0]:
                    pending_count = 0
                    for x in neoctl.getNodeList(node_type=NodeTypes.STORAGE):
                        if x[3] != NodeStates.PENDING:
                            target[0] = None # cluster must start automatically
                            break
                        pending_count += 1
                    if pending_count == target[0]:
                        neoctl.startCluster()
            except (NotReadyException, SystemExit):
                pass
        if not pdb.wait(test, MAX_START_TIME):
            raise AssertionError('Timeout when starting cluster')

    def startCluster(self):
        # Even if the storage nodes are in the expected state, there may still
        # be activity between them and the master, preventing the cluster to
        # start.
        def start(last_try):
            try:
                self.neoctl.startCluster()
            except (NotReadyException, SystemExit) as e:
                return False, e
            return True, None
        self.expectCondition(start)

    def stop(self, clients=True, ignore_errors=False, clear_databases=False):
        # Suspend all processes to kill before actually killing them, so that
        # nodes don't log errors because they get disconnected from other nodes:
        # otherwise, storage nodes would often flush MB of logs just because we
        # killed the master first, and waste much file system space.
        stopped_list = []
        for process_list in six.itervalues(self.process_dict):
            for process in process_list:
                try:
                    process.kill(signal.SIGUSR2)
                    stopped_list.append(process)
                except AlreadyStopped:
                    pass
        for process in stopped_list:
            process.child_coverage()
        error_list = []
        for process in stopped_list:
            try:
                process.kill(signal.SIGKILL)
                process.wait()
            except NodeProcessError as e:
                error_list += e.args
        if clients:
            for zodb_storage in self.zodb_storage_list:
                zodb_storage.close()
            self.zodb_storage_list = []
        self.neoctl.close()
        del self.neoctl
        if clear_databases:
            for db in self.db_list:
                dm = buildDatabaseManager(self.adapter, (self.db_template(db),))
                dm.setup(True)
        time.sleep(0.5)
        if error_list and not ignore_errors:
            raise NodeProcessError('\n'.join(error_list))

    def waitAll(self):
        for process_list in six.itervalues(self.process_dict):
            for process in process_list:
                try:
                    process.wait()
                except (AlreadyStopped, NodeProcessError):
                    pass

    def getClientConfig(self, **kw):
        kw['name'] = self.cluster_name
        kw['master_nodes'] = self.master_nodes.replace('/', ' ')
        if self.SSL:
            kw['ca'], kw['cert'], kw['key'] = self.SSL
        return kw

    def getZODBStorage(self, **kw):
        result = Storage(**self.getClientConfig(**kw))
        result.app.max_reconnection_to_master = 10
        self.zodb_storage_list.append(result)
        return result

    def getZODBConnection(self, **kw):
        """ Return a tuple with the database and a connection """
        db = ZODB.DB(storage=self.getZODBStorage(**kw))
        return (db, db.open())

    def getSQLConnection(self, db):
        assert db is not None and db in self.db_list
        with Patch(manager.DatabaseManager, LOCK=None):
            return buildDatabaseManager(self.adapter, (self.db_template(db),))

    def getMasterProcessList(self):
        return self.process_dict.get(NodeTypes.MASTER)

    def getStorageProcessList(self):
        return self.process_dict.get(NodeTypes.STORAGE)

    def getAdminProcessList(self):
        return self.process_dict.get(NodeTypes.ADMIN)

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
        for node in self.__getNodeList(node_type):
            if node[2] == uuid:
                return node[3]

    def getMasterNodeState(self, uuid):
        return self.__getNodeState(NodeTypes.MASTER, uuid)

    def getPrimary(self):
        try:
            current_try = self.neoctl.getPrimary()
        except NotReadyException:
            current_try = None
        return current_try

    def expectCondition(self, condition, timeout=0, on_fail=None):
        opaque_history = [None]
        def test():
            reached, opaque = condition(opaque_history[-1])
            if not reached:
                opaque_history.append(opaque)
            return reached
        if not pdb.wait(test, timeout + DELAY_SAFETY_MARGIN):
            del opaque_history[0]
            if on_fail is not None:
                on_fail(opaque_history)
            raise AssertionError('Timeout while expecting condition. '
                                 'History: %s' % opaque_history)

    def expectAllMasters(self, node_count, state=None, *args, **kw):
        def callback(last_try):
            try:
                current_try = len(self.getMasterList(state=state))
            except NotReadyException:
                current_try = 0
            if last_try is not None and current_try < last_try:
                raise AssertionError('Regression: %s became %s'
                                     % (last_try, current_try))
            return (current_try == node_count, current_try)
        self.expectCondition(callback, *args, **kw)

    def __expectNodeState(self, node_type, uuid, state, *args, **kw):
        if not isinstance(state, (tuple, list)):
            state = (state, )
        def callback(last_try):
            try:
                current_try = self.__getNodeState(node_type, uuid)
            except NotReadyException:
                current_try = None
            return current_try in state, current_try
        self.expectCondition(callback, *args, **kw)

    def expectMasterState(self, uuid, state, *args, **kw):
        self.__expectNodeState(NodeTypes.MASTER, uuid, state, *args, **kw)

    def expectStorageState(self, uuid, state, *args, **kw):
        self.__expectNodeState(NodeTypes.STORAGE, uuid, state, *args, **kw)

    def expectRunning(self, process, *args, **kw):
        self.expectStorageState(process.getUUID(), NodeStates.RUNNING,
                                *args, **kw)

    def expectPending(self, process, *args, **kw):
        self.expectStorageState(process.getUUID(), NodeStates.PENDING,
                                *args, **kw)

    def expectDown(self, process, *args, **kw):
        self.expectStorageState(process.getUUID(), NodeStates.DOWN, *args, **kw)

    def expectPrimary(self, uuid=None, *args, **kw):
        def callback(last_try):
            current_try = self.getPrimary()
            if None not in (uuid, current_try) and uuid != current_try:
                raise AssertionError('Unexpected primary %r, expected %r'
                                     % (dump(current_try), dump(uuid)))
            return uuid is None or uuid == current_try, current_try
        self.expectCondition(callback, *args, **kw)

    def expectOudatedCells(self, number, *args, **kw):
        def callback(last_try):
            row_list = self.neoctl.getPartitionRowList()[2]
            number_of_outdated = 0
            for row in row_list:
                for cell in row:
                    if cell[1] == CellStates.OUT_OF_DATE:
                        number_of_outdated += 1
            return number_of_outdated == number, number_of_outdated
        self.expectCondition(callback, *args, **kw)

    def expectAssignedCells(self, process, number, *args, **kw):
        def callback(last_try):
            row_list = self.neoctl.getPartitionRowList()[2]
            assigned_cells_number = 0
            for row in row_list:
                for cell in row:
                    if cell[0] == process.getUUID():
                        assigned_cells_number += 1
            return assigned_cells_number == number, assigned_cells_number
        self.expectCondition(callback, *args, **kw)

    def expectClusterState(self, state, *args, **kw):
        def callback(last_try):
            try:
                current_try = self.neoctl.getClusterState()
            except NotReadyException:
                current_try = None
            return current_try == state, current_try
        self.expectCondition(callback, *args, **kw)

    def expectClusterRecovering(self, *args, **kw):
        self.expectClusterState(ClusterStates.RECOVERING, *args, **kw)

    def expectClusterVerifying(self, *args, **kw):
        self.expectClusterState(ClusterStates.VERIFYING, *args, **kw)

    def expectClusterRunning(self, *args, **kw):
        self.expectClusterState(ClusterStates.RUNNING, *args, **kw)

    def expectAlive(self, process, *args, **kw):
        def callback(last_try):
            current_try = process.isAlive()
            return current_try, current_try
        self.expectCondition(callback, *args, **kw)

    def expectDead(self, process, *args, **kw):
        def callback(last_try):
            current_try = not process.isAlive()
            return current_try, current_try
        self.expectCondition(callback, *args, **kw)

    def expectStorageUnknown(self, process, *args, **kw):
        process_uuid = process.getUUID()
        def expected_storage_not_known(last_try):
            for storage in self.getStorageList():
                if storage[2] == process_uuid:
                    return False, storage
            return True, None
        self.expectCondition(expected_storage_not_known, *args, **kw)

    def __del__(self):
        if self.cleanup_on_delete:
            os.removedirs(self.temp_dir)


class NEOFunctionalTest(NeoTestBase):

    __patch = Patch(SocketConnector, CONNECT_LIMIT=0.01) # same as in ClusterPdb

    def setUp(self):
        if random.randint(0, 1):
            NEOCluster.SSL = SSL
        super(NEOFunctionalTest, self).setUp()
        self.__patch.apply()

    def _tearDown(self, success):
        self.__patch.revert()
        super(NEOFunctionalTest, self)._tearDown(success)
        NEOCluster.SSL = None

    def setupLog(self):
        logging.setup(os.path.join(self.getTempDirectory(), 'test.log'))
        logging.resetNids()

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
        thread.daemon = True
        thread.start()
        thread.join(timeout)
        self.assertFalse(thread.is_alive(), 'Run timeout')
        if exc_list:
            assert len(exc_list) == 1, exc_list
            six.reraise(*exc_list[0])
