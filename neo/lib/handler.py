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

import sys
from collections import deque
from operator import itemgetter
from . import logging
from .connection import ConnectionClosed
from .exception import PrimaryElected
from .protocol import (NodeStates, NodeTypes, Packets, uuid_str,
    Errors, BackendNotImplemented, NonReadableCell, NotReadyError,
    PacketMalformedError, ProtocolError, UnexpectedPacketError)
from .util import cached_property


class DelayEvent(Exception):
    pass


class EventHandler(object):
    """This class handles events."""

    def __new__(cls, app, *args, **kw):
        try:
            return app._handlers[cls]
        except AttributeError: # for BackupApplication
            self = object.__new__(cls)
        except KeyError:
            self = object.__new__(cls)
            if cls.__init__ is object.__init__:
                app._handlers[cls] = self
        self.app = app
        return self

    def __repr__(self):
        return self.__class__.__name__

    def __unexpectedPacket(self, conn, packet, message=None):
        """Handle an unexpected packet."""
        if message is None:
            message = 'unexpected packet type %s in %s' % (type(packet),
                    self.__class__.__name__)
        else:
            message = 'unexpected packet: %s in %s' % (message,
                    self.__class__.__name__)
        logging.error(message)
        conn.answer(Errors.ProtocolError(message))
        conn.abort()

    def dispatch(self, conn, packet, kw={}):
        """This is a helper method to handle various packet types."""
        try:
            conn.setPeerId(packet.getId())
            try:
                method = getattr(self, packet.handler_method_name)
            except AttributeError:
                raise UnexpectedPacketError('no handler found')
            args = packet.decode() or ()
            method(conn, *args, **kw)
        except DelayEvent, e:
            assert not kw, kw
            self.getEventQueue().queueEvent(method, conn, args, *e.args)
        except UnexpectedPacketError, e:
            if not conn.isClosed():
                self.__unexpectedPacket(conn, packet, *e.args)
        except PacketMalformedError, e:
            logging.error('malformed packet from %r: %s', conn, e)
            conn.close()
        except NotReadyError, message:
            if not conn.isClosed():
                if not message.args:
                    message = 'Retry Later'
                message = str(message)
                conn.answer(Errors.NotReady(message))
                conn.abort()
        except ProtocolError, message:
            if not conn.isClosed():
                message = str(message)
                conn.answer(Errors.ProtocolError(message))
                conn.abort()
        except BackendNotImplemented, message:
            m = message[0]
            conn.answer(Errors.BackendNotImplemented(
                "%s.%s does not implement %s"
                % (m.im_class.__module__, m.im_class.__name__, m.__name__)))
        except NonReadableCell, e:
            conn.answer(Errors.NonReadableCell())
        except AssertionError:
            e = sys.exc_info()
            try:
                try:
                    conn.close()
                except Exception:
                    logging.exception("")
                raise e[0], e[1], e[2]
            finally:
                del e

    def checkClusterName(self, name):
        # raise an exception if the given name mismatch the current cluster name
        if self.app.name != name:
            logging.error('reject an alien cluster')
            raise ProtocolError('invalid cluster name')


    # Network level handlers

    def packetReceived(self, *args):
        """Called when a packet is received."""
        self.dispatch(*args)

    def connectionStarted(self, conn):
        """Called when a connection is started."""
        logging.debug('connection started for %r', conn)

    def connectionCompleted(self, conn):
        """Called when a connection is completed."""
        logging.debug('connection completed for %r (from %s:%u)',
                      conn, *conn.getConnector().getAddress())

    def connectionFailed(self, conn):
        """Called when a connection failed."""
        logging.debug('connection failed for %r', conn)

    def connectionClosed(self, conn):
        """Called when a connection is closed by the peer."""
        logging.debug('connection closed for %r', conn)
        self.connectionLost(conn, NodeStates.DOWN)

    def connectionLost(self, conn, new_state):
        """ this is a method to override in sub-handlers when there is no need
        to make distinction from the kind event that closed the connection  """
        pass


    # Packet handlers.

    def notPrimaryMaster(self, conn, primary, known_master_list):
        nm = self.app.nm
        for address in known_master_list:
            nm.createMaster(address=address)
        if primary is not None:
            primary = known_master_list[primary]
            assert primary != self.app.server
            raise PrimaryElected(nm.getByAddress(primary))

    def _acceptIdentification(*args):
        pass

    def acceptIdentification(self, conn, node_type, uuid,
                             num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getByAddress(conn.getAddress())
        assert node.getConnection() is conn, (node.getConnection(), conn)
        if node.getType() == node_type:
            if node_type == NodeTypes.MASTER:
                other = app.nm.getByUUID(uuid)
                if other is not None:
                    other.setUUID(None)
                node.setUUID(uuid)
                node.setRunning()
                if your_uuid is None:
                    raise ProtocolError('No UUID supplied')
                logging.info('connected to a primary master node')
                app.setUUID(your_uuid)
                app.id_timestamp = None
            elif node.getUUID() != uuid or app.uuid != your_uuid != None:
                raise ProtocolError('invalid uuids')
            node.setIdentified()
            self._acceptIdentification(node, num_partitions, num_replicas)
            return
        conn.close()

    def notifyNodeInformation(self, conn, *args):
        app = self.app
        app.nm.update(app, *args)

    def ping(self, conn):
        conn.answer(Packets.Pong())

    def pong(self, conn):
        pass

    def closeClient(self, conn):
        conn.server = False
        if not conn.client:
            conn.close()

    def flushLog(self, conn):
        logging.flush()

    # Error packet handlers.

    def error(self, conn, code, message, **kw):
        try:
            getattr(self, Errors[code])(conn, message)
        except (AttributeError, ValueError):
            raise UnexpectedPacketError(message)

    # XXX: For some errors, the connection should have been closed by the remote
    #      peer. But what happens if it's not the case because of some bug ?
    def protocolError(self, conn, message):
        logging.error('protocol error: %s', message)

    def notReadyError(self, conn, message):
        logging.error('not ready: %s', message)

    def timeoutError(self, conn, message):
        logging.error('timeout error: %s', message)

    def ack(self, conn, message):
        logging.debug("no error message: %s", message)

    def backendNotImplemented(self, conn, message):
        raise NotImplementedError(message)


class MTEventHandler(EventHandler):
    """Base class of handler implementations for MTClientConnection"""

    @cached_property
    def dispatcher(self):
        return self.app.dispatcher

    def dispatch(self, conn, packet, kw={}):
        assert conn.lock._is_owned() # XXX: see also lockCheckWrapper
        super(MTEventHandler, self).dispatch(conn, packet, kw)

    def packetReceived(self, conn, packet, kw={}):
        """Redirect all received packet to dispatcher thread."""
        if packet.isResponse():
            if packet.poll_thread:
                self.dispatch(conn, packet, kw)
                kw = {}
            if not (self.dispatcher.dispatch(conn, packet.getId(), packet, kw)
                    or type(packet) is Packets.Pong or conn.isClosed()):
                raise ProtocolError('Unexpected response packet from %r: %r'
                                    % (conn, packet))
        else:
            self.dispatch(conn, packet, kw)

    def connectionLost(self, conn, new_state):
        self.dispatcher.unregister(conn)

    def connectionFailed(self, conn):
        self.dispatcher.unregister(conn)


def unexpectedInAnswerHandler(*args, **kw):
    raise Exception('Unexpected event in an answer handler')

class AnswerBaseHandler(EventHandler):

    connectionStarted = unexpectedInAnswerHandler
    connectionCompleted = unexpectedInAnswerHandler
    connectionFailed = unexpectedInAnswerHandler
    timeoutExpired = unexpectedInAnswerHandler
    connectionClosed = unexpectedInAnswerHandler
    packetReceived = unexpectedInAnswerHandler
    protocolError = unexpectedInAnswerHandler

    def acceptIdentification(*args):
        pass

    def connectionClosed(self, conn):
        raise ConnectionClosed


class _DelayedConnectionEvent(EventHandler):
    # WARNING: This assumes that the connection handler does not change.

    handler_method_name = '_func'
    __new__ = object.__new__

    def __init__(self, func, conn, args):
        self._args = args
        self._conn = conn
        self._func = func
        self._msg_id = conn.getPeerId()

    def __call__(self):
        conn = self._conn
        if not conn.isClosed():
            msg_id = conn.getPeerId()
            try:
                self.dispatch(conn, self)
            finally:
                conn.setPeerId(msg_id)

    def __repr__(self):
        return '<%s: 0x%x %s>' % (self._func.__name__, self._msg_id, self._conn)

    def decode(self):
        return self._args

    def getEventQueue(self):
        raise

    def getId(self):
        return self._msg_id


class EventQueue(object):

    def __init__(self):
        self._event_queue = []
        self._executing_event = -1

    # Stable sort when 2 keys are equal.
    # XXX: Is it really useful to keep events with same key ordered
    #      chronologically ? The caller could use more specific keys. For
    #      write-locks (by the storage node), the locking tid seems enough.
    sortQueuedEvents = (lambda key=itemgetter(0): lambda self:
        self._event_queue.sort(key=key))()

    def queueEvent(self, func, conn=None, args=(), key=None):
        assert self._executing_event < 0, self._executing_event
        self._event_queue.append((key, func if conn is None else
            _DelayedConnectionEvent(func, conn, args)))
        if key is not None:
            self.sortQueuedEvents()

    def sortAndExecuteQueuedEvents(self):
        if self._executing_event < 0:
            self.sortQueuedEvents()
            self.executeQueuedEvents()
        else:
            # We can't sort events when they're being processed.
            self._executing_event = 1

    def executeQueuedEvents(self):
        # Not reentrant. When processing a queued event, calling this method
        # only tells the caller to retry all events from the beginning, because
        # events for the same connection must be processed in chronological
        # order.
        queue = self._event_queue
        if queue: # return quickly if the queue is empty
            self._executing_event += 1
            if self._executing_event:
                return
            done = []
            while 1:
                try:
                    for i, event in enumerate(queue):
                        try:
                            event[1]()
                            done.append(i)
                        except DelayEvent:
                            pass
                        if self._executing_event:
                            break
                    else:
                        break
                finally:
                    while done:
                        del queue[done.pop()]
                self._executing_event = 0
                # What sortAndExecuteQueuedEvents could not do immediately
                # is done here:
                if event[0] is not None:
                    self.sortQueuedEvents()
            self._executing_event = -1

    def logQueuedEvents(self):
        if self._event_queue:
            logging.info(" Pending events:")
            for event in self._event_queue:
                logging.info('  %r', event)
