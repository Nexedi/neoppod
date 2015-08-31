#
# Copyright (C) 2006-2019  Nexedi SA
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

import thread
import weakref
from copy import deepcopy
from collections import defaultdict
from json import dumps as json_dumps
from logging import ERROR, INFO
from wsgiref.simple_server import WSGIRequestHandler
from bottle import Bottle, HTTPError, request, response
from . import handler
from neo.lib import logging
from neo.lib.app import buildOptionParser
from neo.lib.exception import PrimaryFailure
from neo.lib.bootstrap import BootstrapManager
from neo.lib.protocol import ClusterStates, NodeStates, NodeTypes, Packets
from neo.lib.pt import PartitionTable
from neo.lib.threaded_app import ThreadedApplication
from neo.lib.util import p64, tidFromTime, u64

def dry_run():
    return bool(int(request.query.dry_run))

def json(x):
    response.content_type = 'application/json'
    return json_dumps(x)

def rowsForJson(row_list):
    x = []
    for row in row_list:
        y = defaultdict(list)
        for uuid, state in row:
            y[state].append(uuid)
        x.append(y)
    return x

def raiseNotReady():
    raise HTTPError(503, 'Not connected to a primary master')


class Bottle(Bottle):

    def default_error_handler(self, res):
        return res.body


class ObjectBottle(object):

    def __init__(self, weakref=False, *args, **kw):
        self._app = Bottle(*args, **kw)
        self._weakref = weakref

    def __getattr__(self, name):
        return getattr(self._app, name)

    def __get__(self, obj, cls):
        if obj is None: return self
        app = obj.bottle = deepcopy(self._app)
        if self._weakref:
            obj = weakref.ref(obj)
            app.install(lambda f: lambda *a, **k: f(obj(), *a, **k))
        else:
            app.install(lambda f: lambda *a, **k: f(obj, *a, **k))
        return app


class RequestHandler(WSGIRequestHandler):

    def _log(self, level, format, *args):
        logging.log(level, "%s %s", self.client_address[0], format % args)

    def log_error(self, *args):
        self._log(ERROR, *args)

    def log_message(self, *args):
        self._log(INFO, *args)


@buildOptionParser
class Application(ThreadedApplication):
    """The storage node application."""

    bottle = ObjectBottle(weakref=True)
    cluster_state = None

    @classmethod
    def _buildOptionParser(cls):
        _ = cls.option_parser
        _.description = "NEO Admin node"
        cls.addCommonServerOptions('admin', '127.0.0.1:9999')

        _ = _.group('admin')
        _.int('i', 'nid',
            help="specify an NID to use for this process (testing purpose)")

    def __init__(self, config):
        super(Application, self).__init__(
            config['masters'], config['cluster'],
            ssl=config.get('ssl'),
            dynamic_master_list=config.get('dynamic_master_list'))
        self.http = config['bind']
        logging.debug('IP address is %s, port is %d', *self.http)
        self.uuid = config.get('nid')
        logging.node(self.name, self.uuid)
        self.master_bootstrap_handler = handler.MasterBootstrapHandler(self)
        self.master_event_handler = handler.MasterEventHandler(self)
        self.primary_handler = handler.PrimaryAnswersHandler(self)

    def _run(self):
        """Make sure that the status is sane and start a loop."""
        try:
            poll = self.em.poll
            while self.cluster_state != ClusterStates.STOPPING:
                self.connectToPrimary()
                try:
                    while True:
                        poll(1)
                except PrimaryFailure:
                    self.nm.log()
                    logging.error('primary master is down')
                finally:
                    self.master_conn = None
            while not self.em.isIdle():
                poll(1)
        finally:
            self.interrupt_main()

    interrupt_main = staticmethod(thread.interrupt_main)

    def connectToPrimary(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.
        """
        self.pt = PartitionTable(0, 0)
        self.master_node = None
        self.uuid = None
        self.cluster_state = None
        bootstrap = BootstrapManager(self, NodeTypes.ADMIN)
        self.master_node, conn = bootstrap.getPrimaryConnection()
        conn.setHandler(self.master_bootstrap_handler)
        conn.ask(Packets.AskClusterState())
        conn.convertToMT(self.dispatcher)
        conn.setHandler(self.master_event_handler)
        self.master_conn = conn

    def _askPrimary(self, packet, **kw):
        """ Send a request to the primary master and process its answer """
        return self._ask(self._getMasterConnection(), packet,
                         handler=self.primary_handler, **kw)

    def _getMasterConnection(self):
        conn = self.master_conn
        if conn is None or conn.isClosed():
            raiseNotReady()
        return conn

    def serve(self, **kw):
        self.start()
        host, port = self.http
        self.bottle.run(server='wsgiref', handler_class=RequestHandler,
                        quiet=1, host=host, port=port)

    def asTID(self, value):
        if '.' in value:
            return tidFromTime(float(value))
        return p64(int(value, 0))

    def queryStorageList(self, allow_all=True):
        node_list = request.query.node_list
        if allow_all and node_list == 'all':
            return [node.getUUID() for node in self.nm.getStorageList()]
        return map(int, request.query.node_list.split(',')) if node_list else ()

    @bottle.route('/getClusterState')
    def getClusterState(self):
        if self.cluster_state is not None:
            return str(self.cluster_state)

    def _setClusterState(self, state):
        return self._askPrimary(Packets.SetClusterState(state))

    @bottle.route('/setClusterState')
    def setClusterState(self):
        self._setClusterState(getattr(ClusterStates, request.query.state))

    @bottle.route('/startCluster')
    def startCluster(self):
        self._setClusterState(ClusterStates.VERIFYING)

    @bottle.route('/enableStorageList')
    def enableStorageList(self):
        return self._askPrimary(Packets.AddPendingNodes(
            self.queryStorageList()))

    @bottle.route('/tweakPartitionTable')
    def tweakPartitionTable(self):
        changed, row_list = self._askPrimary(Packets.TweakPartitionTable(
            dry_run(), self.queryStorageList(False)))
        return json((changed, rowsForJson(row_list)))

    @bottle.route('/setNumReplicas')
    def setNumReplicas(self):
        return self._askPrimary(Packets.SetNumReplicas(int(request.query.nr)))

    @bottle.route('/getLastIds')
    def getLastIds(self):
        loid, ltid = self._askPrimary(Packets.AskLastIDs())
        return json((u64(loid), u64(ltid)))

    @bottle.route('/getLastTransaction')
    def getLastTransaction(self):
        return self._askPrimary(Packets.AskLastTransaction())

    @bottle.route('/getRecovery')
    def getRecovery(self):
        ptid, backup_tid, truncate_tid = self._askPrimary(Packets.AskRecovery())
        return json((ptid,
            backup_tid and u64(backup_tid),
            truncate_tid and u64(truncate_tid),
            ))

    @bottle.route('/getNodeList')
    def getNodeList(self):
        node_type = request.query.node_type
        if node_type:
            node_type = getattr(NodeTypes, node_type)
            node_filter = lambda node: node.getType() is node_type
        else:
            node_filter = None
        node_list = []
        self._getMasterConnection()
        for node in self.nm.getList(node_filter):
            node_type, address, uuid, state, id_timestamp = \
                node = node.asTuple()
            node_list.append((str(node_type), address, uuid,
                              str(state), id_timestamp))
        return json(node_list)

    @bottle.route('/getPrimary')
    def getPrimary(self):
        return str(getattr(self.master_node, 'getUUID', raiseNotReady)())

    def _setNodeState(self, node, state):
        return self._askPrimary(Packets.SetNodeState(node, state))

    @bottle.route('/dropNode')
    def dropNode(self):
        self._setNodeState(int(request.query.node), NodeStates.UNKNOWN)

    @bottle.route('/killNode')
    def killNode(self):
        self._setNodeState(int(request.query.node), NodeStates.DOWN)

    @bottle.route('/getPartitionRowList')
    def getPartitionRowList(self):
        q = request.query
        min_offset = q.min_offset
        max_offset = q.max_offset
        uuid = q.node
        pt = self.pt
        try:
            min_offset = int(min_offset) if min_offset else 0
            max_offset = max_offset and int(max_offset) or pt.getPartitions()
            uuid = int(uuid) if uuid else None
        except ValueError, e:
            raise HTTPError(400, str(e))
        row_list = []
        try:
            for offset in xrange(min_offset, max_offset):
                row = []
                try:
                    for cell in pt.getCellList(offset):
                        if uuid is None or cell.getUUID() == uuid:
                            row.append((cell.getUUID(), cell.getState()))
                except TypeError:
                    pass
                row_list.append(row)
        except IndexError:
            raise HTTPError(400, 'invalid partition table offset')
        return json((pt.getID(), pt.getReplicas(), rowsForJson(row_list)))

    @bottle.route('/repair')
    def repair(self):
        return self._askPrimary(Packets.Repair(
            self.queryStorageList(), dry_run()))

    @bottle.route('/truncate')
    def truncate(self):
        return self._askPrimary(Packets.Truncate(self.asTID(request.query.tid)))

    @bottle.route('/checkReplicas')
    def checkReplicas(self):
        partition_dict = {}
        for partition in request.query.pt.split(','):
            partition, source = partition.split(':')
            source = int(source) if source else None
            if partition:
                partition_dict[int(partition)] = source
            elif partition_dict:
                raise HTTPError(400)
            else:
                self._getMasterConnection() # just for correct error handling
                partition_dict = dict.fromkeys(xrange(self.pt.getPartitions()),
                                               source)
        max_tid = request.query.max_tid
        return self._askPrimary(Packets.CheckReplicas(partition_dict,
            self.asTID(request.query.min_tid),
            self.asTID(max_tid) if max_tid else None))
