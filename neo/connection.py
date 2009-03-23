#
# Copyright (C) 2006-2009  Nexedi SA
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

import logging
from threading import RLock

from neo.protocol import Packet, ProtocolError
from neo.event import IdleEvent
from neo.connector import *

class BaseConnection(object):
    """A base connection."""

    def __init__(self, event_manager, handler, connector = None,
                 addr = None, connector_handler = None):
        self.em = event_manager
        self.connector = connector
        self.addr = addr
        self.handler = handler
        if connector is not None:
            self.connector_handler = connector.__class__
            event_manager.register(self)
        else:            
            self.connector_handler = connector_handler
            
    def lock(self):
        return 1

    def unlock(self):
        return None

    def getConnector(self):
        return self.connector

    def getDescriptor(self):
        return self.connector.getDescriptor()

    def setConnector(self, connector):
        if self.connector is not None:
            raise RuntimeError, 'cannot overwrite a connector in a connection'
        if connector is not None:
            self.connector = connector
            self.em.register(self)

    def getAddress(self):
        return self.addr

    def readable(self):
        raise NotImplementedError

    def writable(self):
        raise NotImplementedError

    def getHandler(self):
        return self.handler

    def setHandler(self, handler):
        self.handler = handler

    def getEventManager(self):
        return self.em

    def getUUID(self):
        return None

class ListeningConnection(BaseConnection):
    """A listen connection."""
    def __init__(self, event_manager, handler, addr = None,
                 connector_handler = None, **kw):
        logging.info('listening to %s:%d', *addr)
        BaseConnection.__init__(self, event_manager, handler,
                                addr = addr,
                                connector_handler = connector_handler)
        connector = self.connector_handler()
        connector.makeListeningConnection(addr)
        self.setConnector(connector)
        self.em.addReader(self)

    def readable(self):
        try:
            new_s, addr = self.connector.getNewConnection()
            logging.info('accepted a connection from %s:%d', *addr)
            self.handler.connectionAccepted(self, new_s, addr)
        except ConnectorTryAgainException:
            pass
            
class Connection(BaseConnection):
    """A connection."""
    def __init__(self, event_manager, handler,
                 connector = None, addr = None,
                 connector_handler = None):
        self.read_buf = []
        self.write_buf = []
        self.cur_id = 0
        self.event_dict = {}
        self.aborted = False
        self.uuid = None
        BaseConnection.__init__(self, event_manager, handler,
                                connector = connector, addr = addr,
                                connector_handler = connector_handler)
        if connector is not None:
            event_manager.addReader(self)

    def getUUID(self):
        return self.uuid

    def setUUID(self, uuid):
        self.uuid = uuid

    def getNextId(self):
        next_id = self.cur_id
        # Deal with an overflow.
        if self.cur_id == 0xffffffff:
            self.cur_id = 0
        else:
            self.cur_id += 1
        return next_id

    def close(self):
        """Close the connection."""
        em = self.em
        if self.connector is not None:
            logging.debug('closing a connector for %s:%d', *(self.addr))
            em.removeReader(self)
            em.removeWriter(self)
            em.unregister(self)            
            self.connector.shutdown()
            self.connector.close()
            self.connector = None
            for event in self.event_dict.itervalues():
                em.removeIdleEvent(event)
            self.event_dict.clear()

    def __del__(self):
        self.close()

    def abort(self):
        """Abort dealing with this connection."""
        logging.debug('aborting a connetor for %s:%d', *(self.addr))
        self.aborted = True

    def writable(self):
        """Called when self is writable."""
        self.send()
        if not self.pending():
            if self.aborted:
                self.close()
            else:
                self.em.removeWriter(self)

    def readable(self):
        """Called when self is readable."""
        self.recv()
        self.analyse()

        if self.aborted:
            self.em.removeReader(self)

    def analyse(self):
        """Analyse received data."""
        if self.read_buf:
            if len(self.read_buf) == 1:
                msg = self.read_buf[0]
            else:
                msg = ''.join(self.read_buf)

            while 1:
                try:
                    packet = Packet.parse(msg)
                except ProtocolError, m:
                    self.handler.packetMalformed(self, *m)
                    return

                if packet is None:
                    break

                # Remove idle events, if appropriate packets were received.
                for msg_id in (None, packet.getId()):
                    try:
                        event = self.event_dict[msg_id]
                        del self.event_dict[msg_id]
                        self.em.removeIdleEvent(event)
                    except KeyError:
                        pass

                self.handler.packetReceived(self, packet)
                msg = msg[len(packet):]

            if msg:
                self.read_buf = [msg]
            else:
                del self.read_buf[:]

    def pending(self):
        return self.connector is not None and len(self.write_buf) != 0

    def recv(self):
        """Receive data from a connector."""
        try:
            r = self.connector.receive()
            if not r:
                logging.error('cannot read')
                self.handler.connectionClosed(self)
                self.close()
            else:
                self.read_buf.append(r)
        except ConnectorTryAgainException:
            pass
        except:
            self.handler.connectionClosed(self)
            self.close()

    def send(self):
        """Send data to a connector."""
        if self.write_buf:
            if len(self.write_buf) == 1:
                msg = self.write_buf[0]
            else:
                msg = ''.join(self.write_buf)
            try:
                r = self.connector.send(msg)
                if not r:
                    logging.error('cannot write')
                    self.handler.connectionClosed(self)
                    self.close()
                elif r == len(msg):
                    del self.write_buf[:]
                else:
                    self.write_buf = [msg[r:]]
            except ConnectorTryAgainException:
                return
            except:
                self.handler.connectionClosed(self)
                self.close()

    def addPacket(self, packet):
        """Add a packet into the write buffer."""
        if self.connector is None:
            return

        try:
            self.write_buf.append(packet.encode())
        except ProtocolError, m:
            logging.critical('trying to send a too big message')
            return self.addPacket(packet.internalError(packet.getId(), m[1]))

        # If this is the first time, enable polling for writing.
        if len(self.write_buf) == 1:
            self.em.addWriter(self)

    def expectMessage(self, msg_id = None, timeout = 5, additional_timeout = 30):
        """Expect a message for a reply to a given message ID or any message.

        The purpose of this method is to define how much amount of time is
        acceptable to wait for a message, thus to detect a down or broken
        peer. This is important, because one error may halt a whole cluster
        otherwise. Although TCP defines a keep-alive feature, the timeout
        is too long generally, and it does not detect a certain type of reply,
        thus it is better to probe problems at the application level.

        The message ID specifies what ID is expected. Usually, this should
        be identical with an ID for a request message. If it is None, any
        message is acceptable, so it can be used to check idle time.

        The timeout is the amount of time to wait until keep-alive messages start.
        Once the timeout is expired, the connection starts to ping the peer.

        The additional timeout defines the amount of time after the timeout
        to invoke a timeoutExpired callback. If it is zero, no ping is sent, and
        the callback is executed immediately."""
        if self.connector is None:
            return

        event = IdleEvent(self, msg_id, timeout, additional_timeout)
        self.event_dict[msg_id] = event
        self.em.addIdleEvent(event)

class ClientConnection(Connection):
    """A connection from this node to a remote node."""
    def __init__(self, event_manager, handler, addr = None,
                 connector_handler = None, **kw):
        self.connecting = True
        Connection.__init__(self, event_manager, handler, addr = addr,
                            connector_handler = connector_handler)
        handler.connectionStarted(self)
        try:
            connector = self.connector_handler()
            self.setConnector(connector)
            try:
                connector.makeClientConnection(addr)
            except ConnectorInProgressException:
                event_manager.addWriter(self)
            else:
                self.connecting = False
                self.handler.connectionCompleted(self)
                event_manager.addReader(self)
        except:
            handler.connectionFailed(self)
            self.close()

    def writable(self):
        """Called when self is writable."""
        if self.connecting:
            err = self.connector.getError()
            if err:
                self.handler.connectionFailed(self)
                self.close()
                return
            else:
                self.connecting = False
                self.handler.connectionCompleted(self)
                self.em.addReader(self)
        else:
            Connection.writable(self)

class ServerConnection(Connection):
    """A connection from a remote node to this node."""
    pass

class MTClientConnection(ClientConnection):
    """A Multithread-safe version of ClientConnection."""
    def __init__(self, *args, **kwargs):
        lock = RLock()
        self.acquire = lock.acquire
        self.release = lock.release
        super(MTClientConnection, self).__init__(*args, **kwargs)

    def lock(self, blocking = 1):
        return self.acquire(blocking = blocking)

    def unlock(self):
        self.release()

class MTServerConnection(ServerConnection):
    """A Multithread-safe version of ServerConnection."""
    def __init__(self, *args, **kwargs):
        lock = RLock()
        self.acquire = lock.acquire
        self.release = lock.release
        super(MTServerConnection, self).__init__(*args, **kwargs)

    def lock(self, blocking = 1):
        return self.acquire(blocking = blocking)

    def unlock(self):
        self.release()

