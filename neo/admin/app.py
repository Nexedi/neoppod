#
# Copyright (C) 2006-2015  Nexedi SA
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

import json
import thread
import weakref
from bottle import Bottle, HTTPError, request, response
from copy import deepcopy
from logging import ERROR, INFO
from wsgiref.simple_server import WSGIRequestHandler
from . import handler
from neo.lib import logging
from neo.lib.exception import PrimaryFailure
from neo.lib.bootstrap import BootstrapManager
from neo.lib.pt import PartitionTable
from neo.lib.protocol import ClusterStates, NodeTypes, NodeStates, Packets
from neo.lib.threaded_app import ThreadedApplication
from neo.lib.util import p64, tidFromTime

def raiseNotReady():
    raise HTTPError(503, 'Not connected to a primary master')


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


class Application(ThreadedApplication):
    """The storage node application."""

    bottle = ObjectBottle(weakref=True)
    cluster_state = None

    def __init__(self, config):
        super(Application, self).__init__(config.getMasters(),
                                          config.getCluster(),
                                          config.getDynamicMasterList())
        self.master_event_handler = handler.MasterEventHandler(self)
        self.notifications_handler = handler.MasterNotificationsHandler(self)
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
        self.master_node = None
        self.uuid = None
        self.cluster_state = None
        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, self.name, NodeTypes.ADMIN)
        (self.master_node, self.master_conn, self.uuid,
         num_partitions, num_replicas) = bootstrap.getPrimaryConnection()
        self.pt = PartitionTable(num_partitions, num_replicas)

        self.master_conn.setHandler(self.master_event_handler)
        self.master_conn.ask(Packets.AskClusterState())
        self.master_conn.ask(Packets.AskNodeInformation())
        self.master_conn.ask(Packets.AskPartitionTable())
        self.master_conn.setHandler(self.notifications_handler)
        self.master_conn.convertToMT(self.dispatcher)

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
        self.bottle.run(server='wsgiref', handler_class=RequestHandler,
                        quiet=1, **kw)

    def asTID(self, value):
        if '.' in value:
            return tidFromTime(float(value))
        return p64(int(value, 0))

    @bottle.route('/getClusterState')
    def getClusterState(self):
        if self.cluster_state is not None:
            return str(self.cluster_state)

    def _setClusterState(self, state):
        self._askPrimary(Packets.SetClusterState(state))

    @bottle.route('/setClusterState')
    def setClusterState(self):
        self._setClusterState(getattr(ClusterStates, request.query.state))

    @bottle.route('/startCluster')
    def startCluster(self):
        self._setClusterState(ClusterStates.VERIFYING)

    @bottle.route('/enableStorageList')
    def enableStorageList(self):
        node_list = request.query.node_list
        self._askPrimary(Packets.AddPendingNodes(map(int,
            request.query.node_list.split(',')) if node_list else ()))

    @bottle.route('/tweakPartitionTable')
    def tweakPartitionTable(self):
        node_list = request.query.node_list
        self._askPrimary(Packets.TweakPartitionTable(map(int,
            request.query.node_list.split(',')) if node_list else ()))

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
            node_type, address, uuid, state = node = node.asTuple()
            node_list.append((str(node_type), address, uuid, str(state)))
        response.content_type = 'application/json'
        return json.dumps(node_list)

    @bottle.route('/getPrimary')
    def getPrimary(self):
        return str(getattr(self.master_node, 'getUUID', raiseNotReady)())

    def _setNodeState(self, node, state):
        self._askPrimary(Packets.SetNodeState(node, state))

    @bottle.route('/killNode')
    def killNode(self):
        self._setNodeState(int(request.query.node), NodeStates.UNKNOWN)

    @bottle.route('/dropNode')
    def killNode(self):
        self._setNodeState(int(request.query.node), NodeStates.DOWN)

    @bottle.route('/getPartitionRowList')
    def getPartitionRowList(self):
        min_offset = int(request.query.min_offset)
        max_offset = int(request.query.max_offset)
        uuid = request.query.node
        uuid = int(uuid) if uuid else None
        row_list = []
        if max_offset == 0:
            max_offset = self.pt.getPartitions()
        try:
            for offset in xrange(min_offset, max_offset):
                row = []
                try:
                    for cell in self.pt.getCellList(offset):
                        if uuid is None or cell.getUUID() == uuid:
                            row.append((cell.getUUID(), str(cell.getState())))
                except TypeError:
                    pass
                row_list.append((offset, row))
        except IndexError:
            raise HTTPError(400, 'invalid partition table offset')
        response.content_type = 'application/json'
        return json.dumps((self.pt.getID(), row_list))

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
        self._askPrimary(Packets.CheckReplicas(partition_dict,
            self.asTID(request.query.min_tid),
            self.asTID(max_tid) if max_tid else None))
