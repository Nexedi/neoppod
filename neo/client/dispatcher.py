from threading import Thread
from Queue import Empty, Queue

from neo.protocol import PING, Packet, CLIENT_NODE_TYPE
from neo.connection import ClientConnection
from neo.node import MasterNode

from time import time
import logging

class Dispatcher(Thread):
    """Dispatcher class use to redirect request to thread."""

    def __init__(self, em, message_queue, request_queue, **kw):
        Thread.__init__(self, **kw)
        self._message_queue = message_queue
        self._request_queue = request_queue
        self.em = em
        # Queue of received packet that have to be processed
        self.message = Queue()
        # This dict is used to associate conn/message id to client thread queue
        # and thus redispatch answer to the original thread
        self.message_table = {}
        # Indicate if we are in process of connection to master node
        self.connecting_to_master_node = 0

    def run(self):
        while 1:
            # First check if we receive any new message from other node
            m = None
            try:
                self.em.poll(0.02)
            except KeyError:
                # This happen when there is no connection
                logging.error('Dispatcher, run, poll returned a KeyError')
            while 1:
                try:
                    conn, packet =  self.message.get_nowait()
                except Empty:
                    break
                # Send message to waiting thread
                key = "%s-%s" %(conn.getUUID(),packet.getId())
                #logging.info('dispatcher got packet %s' %(key,))
                if self.message_table.has_key(key):
                    tmp_q = self.message_table.pop(key)
                    tmp_q.put((conn, packet), True)
                else:
                    #conn, packet = self.message
                    method_type = packet.getType()
                    if method_type == PING:
                        # must answer with no delay
                        conn.addPacket(Packet().pong(packet.getId()))
                    else:
                        # put message in request queue
                        self._request_queue.put((conn, packet), True)

            # Then check if a client ask me to send a message
            try:
                m = self._message_queue.get_nowait()
                if m is not None:
                    tmp_q, msg_id, conn, p = m
                    conn.addPacket(p)
                    if tmp_q is not None:
                        # We expect an answer
                        key = "%s-%s" %(conn.getUUID(),msg_id)
                        self.message_table[key] = tmp_q
                        conn.expectMessage(msg_id)
            except Empty:
                continue

    def connectToPrimaryMasterNode(self, app):
        """Connect to a primary master node.
        This can be called either at bootstrap or when
        client got disconnected during process"""
        # Indicate we are trying to connect to avoid multiple try a time
        self.connecting_to_master_node = 1
        from neo.client.handler import ClientEventHandler
        if app.pt is not None:
            app.pt.clear()
        master_index = 0
        conn = None
        # Make application execute remaining message if any
        app._waitMessage(block=0)
        handler = ClientEventHandler(app, app.dispatcher)
        while 1:
            if app.pt is not None and app.pt.operational():
                # Connected to primary master node and got all informations
                break
            app.node_not_ready = 0
            if app.primary_master_node is None:
                # Try with master node defined in config
                addr, port = app.master_node_list[master_index].split(':')
                port = int(port)
            else:
                addr, port = app.primary_master_node.getServer()
            # Request Node Identification
            conn = ClientConnection(app.em, handler, (addr, port))
            if app.nm.getNodeByServer((addr, port)) is None:
                n = MasterNode(server = (addr, port))
                app.nm.add(n)
            msg_id = conn.getNextId()
            p = Packet()
            p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, app.uuid,
                                        '0.0.0.0', 0, app.name)
            # Send message
            conn.addPacket(p)
            conn.expectMessage(msg_id)
            app.local_var.tmp_q = Queue(1)
            # Wait for answer
            while 1:
                try:
                    self.em.poll(1)
                except TypeError:
                    t = time()
                    while time() < t + 1:
                        pass
                    break
                # Check if we got a reply
                try:
                    conn, packet =  self.message.get_nowait()
                    method_type = packet.getType()
                    if method_type == PING:
                        # Must answer with no delay
                        conn.addPacket(Packet().pong(packet.getId()))
                        break
                    else:
                        # Process message by handler
                        conn.handler.dispatch(conn, packet)
                except Empty:
                    pass

                # Now check result
                if app.primary_master_node is not None:
                    if app.primary_master_node == -1:
                        # Connection failed, try with another master node
                        app.primary_master_node = None
                        master_index += 1
                        break
                    elif app.primary_master_node.getServer() != (addr, port):
                        # Master node changed, connect to new one
                        break
                    elif app.node_not_ready:
                        # Wait a bit and reask again
                        t = time()
                        while time() < t + 1:
                            pass
                        break
                    elif app.pt is not None and app.pt.operational():
                        # Connected to primary master node
                        break

                # If nothing, check if we have new message to send
                try:
                    m = self._message_queue.get_nowait()
                    if m is not None:
                        tmp_q, msg_id, conn, p = m
                        conn.addPacket(p)
                except Empty:
                    continue

        logging.info("connected to primary master node %s %d" %app.primary_master_node.getServer())
        app.master_conn = conn
        self.connecting_to_master_node = 0
