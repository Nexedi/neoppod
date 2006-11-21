import socket
import errno
import logging
from select import select
from time import time

from protocol import Packet, ProtocolError

class IdleEvent:
    """This class represents an event called when a connection is waiting for
    a message too long."""

    def __init__(self, conn, msg_id, timeout, additional_timeout):
        self._conn = conn
        self._id = msg_id
        t = time()
        self._time = t + timeout
        self._critical_time = t + timeout + additional_timeout
        self._additional_timeout = additional_timeout

    def getId(self):
        return self._id

    def getTime(self):
        return self._time

    def getCriticalTime(self):
        return self._critical_time

    def __call__(self, t):
        conn = self._conn
        if t > self._critical_time:
            logging.info('timeout with %s:%d', conn.ip_address, conn.port)
            self._conn.timeoutExpired(self)
            return True
        elif t > self._time:
            if self._additional_timeout > 10:
                self._additional_timeout -= 10
                conn.expectMessage(self._id, 10, self._additional_timeout)
                # Start a keep-alive packet.
                logging.info('sending a ping to %s:%d', conn.ip_address, conn.port)
                msg_id = conn.getNextId()
                conn.addPacket(Packet().ping(msg_id))
                conn.expectMessage(msg_id, 10, 0)
            else:
                conn.expectMessage(self._id, self._additional_timeout, 0)
            return True
        return False


class Connection:
    """A connection."""

    connecting = False
    from_self = False
    aborted = False

    def __init__(self, connection_manager, s = None, addr = None):
        self.s = s
        if s is not None:
            connection_manager.addReader(s)
        self.cm = connection_manager
        self.read_buf = []
        self.write_buf = []
        self.cur_id = 0
        self.event_dict = {}
        if addr is None:
            self.ip_address = None
            self.port = None
        else:
            self.ip_address, self.port = addr

    def getSocket(self):
        return self.s

    def getNextId(self):
        next_id = self.cur_id
        self.cur_id += 1
        if self.cur_id > 0xffff:
            self.cur_id = 0
        return next_id

    def connect(self, ip_address, port):
        """Connect to another node."""
        if self.s is not None:
            raise RuntimeError, 'already connected'

        self.ip_address = ip_address
        self.port = port
        self.from_self = True

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                s.setblocking(0)
                s.connect((ip_address, port))
            except socket.error, m:
                if m[0] == errno.EINPROGRESS:
                    self.connecting = True
                    self.cm.addWriter(s)
                else:
                    s.close()
                    raise
            else:
                self.connectionCompleted()
                self.cm.addReader(s)
        except socket.error:
            self.connectionFailed()
            return

        self.s = s
        return s

    def close(self):
        """Close the connection."""
        s = self.s
        if s is not None:
            logging.debug('closing a socket for %s:%d', self.ip_address, self.port)
            self.cm.removeReader(s)
            self.cm.removeWriter(s)
            self.cm.unregister(self)
            try:
                # This may fail if the socket is not connected.
                s.shutdown(socket.SHUT_RDWR)
            except socket.error:
                pass
            s.close()
            self.s = None
            for event in self.event_dict.itervalues():
                self.cm.removeIdleEvent(event)
            self.event_dict.clear()
            self.connectionClosed()

    def abort(self):
        """Abort dealing with this connection."""
        logging.debug('aborting a socket for %s:%d', self.ip_address, self.port)
        self.aborted = True

    def writable(self):
        """Called when self is writable."""
        if self.connecting:
            err = self.s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                self.connectionFailed()
                self.close()
                return
            else:
                self.connecting = False
                self.connectionCompleted()
                self.cm.addReader(self.s)
        else:
            self.send()

        if not self.pending():
            if self.aborted:
                self.close()
            else:
                self.cm.removeWriter(self.s)

    def readable(self):
        """Called when self is readable."""
        self.recv()
        self.analyse()

        if self.aborted:
            self.cm.removeReader(self.s)

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
                    self.packetMalformed(*m)
                    return

                if packet is None:
                    break

                # Remove idle events, if appropriate packets were received.
                for msg_id in (None, packet.getId()):
                    try:
                        event = self.event_dict[msg_id]
                        del self.event_dict[msg_id]
                        self.cm.removeIdleEvent(event)
                    except KeyError:
                        pass

                self.packetReceived(packet)
                msg = msg[len(packet)]

            if msg:
                self.read_buf = [msg]
            else:
                del self.read_buf[:]

    def pending(self):
        return self.s is not None and len(self.write_buf) != 0

    def recv(self):
        """Receive data from a socket."""
        s = self.s
        try:
            r = s.recv(4096)
            if not r:
                logging.error('cannot read')
                self.close()
            else:
                self.read_buf.append(r)
        except socket.error, m:
            if m[0] == errno.EAGAIN:
                return []
            raise

    def send(self):
        """Send data to a socket."""
        s = self.s
        if self.write_buf:
            if len(self.write_buf) == 1:
                msg = self.write_buf[0]
            else:
                msg = ''.join(self.write_buf)
            try:
                r = s.send(msg)
                if not r:
                    logging.error('cannot write')
                    self.close()
                elif r == len(msg):
                    del self.write_buf[:]
                else:
                    self.write_buf = [msg[:r]]
            except socket.error, m:
                if m[0] == errno.EAGAIN:
                    return
                raise

    def addPacket(self, packet):
        """Add a packet into the write buffer."""
        try:
            self.write_buf.append(str(packet))
        except ProtocolError, m:
            logging.critical('trying to send a too big message')
            return self.addPacket(Packet().internalError(packet.getId(), m[1]))

        # If this is the first time, enable polling for writing.
        if len(self.write_buf) == 1:
            self.cm.addWriter(self.s)

    def expectMessage(self, msg_id = None, timeout = 10, additional_timeout = 100):
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
        event = IdleEvent(self, msg_id, timeout, additional_timeout)
        self.event_dict[msg_id] = event
        self.cm.addIdleEvent(event)

    # Hooks.
    def connectionFailed(self):
        """Called when a connection fails."""
        pass

    def connectionCompleted(self):
        """Called when a connection is completed."""
        pass

    def connectionAccepted(self):
        """Called when a connection is accepted."""
        # A request for a node identification should arrive.
        self.expectMessage(timeout = 10, additional_timeout = 0)

    def connectionClosed(self):
        """Called when a connection is closed."""
        pass

    def timeoutExpired(self):
        """Called when a timeout event occurs."""
        self.close()

    def peerBroken(self):
        """Called when a peer is broken."""
        pass

    def packetReceived(self, packet):
        """Called when a packet is received."""
        pass

    def packetMalformed(self, packet, error_message):
        """Called when a packet is malformed."""
        self.peerBroken()
        self.addPacket(Packet().protocolError(packet.getId(), error_message))
        self.abort()

class ConnectionManager:
    """This class manages connections and sockets."""

    def __init__(self, app = None, connection_klass = Connection):
        self.listening_socket = None
        self.connection_dict = {}
        self.reader_set = set([])
        self.writer_set = set([])
        self.exc_list = []
        self.app = app
        self.klass = connection_klass
        self.event_list = []
        self.prev_time = time()

    def listen(self, ip_address, port):
        logging.info('listening to %s:%d', ip_address, port)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(0)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((ip_address, port))
        s.listen(5)
        self.listening_socket = s
        self.reader_set.add(s)

    def register(self, conn):
        self.connection_dict[conn.getSocket()] = conn

    def unregister(self, conn):
        del self.connection_dict[conn.getSocket()]

    def connect(self, ip_address, port):
        logging.info('connecting to %s:%d', ip_address, port)
        conn = self.klass(self)
        if conn.connect(ip_address, port) is not None:
            self.register(conn)

    def poll(self, timeout = 1):
        rlist, wlist, xlist = select(self.reader_set, self.writer_set, self.exc_list,
                                     timeout)
        for s in rlist:
            if s == self.listening_socket:
                try:
                    conn, addr = s.accept()
                    logging.info('accepted a connection from %s:%d', addr[0], addr[1])
                    self.register(self.klass(self, conn, addr))
                except socket.error, m:
                    if m[0] == errno.EAGAIN:
                        continue
                    raise
            else:
                conn = self.connection_dict[s]
                conn.readable()

        for s in wlist:
            conn = self.connection_dict[s]
            conn.writable()

        # Check idle events. Do not check them out too often, because this
        # is somehow heavy.
        event_list = self.event_list
        if event_list:
            t = time()
            if t - self.prev_time >= 1:
                self.prev_time = t
                event_list.sort(key = lambda event: event.getTimeout())
                for event in tuple(event_list):
                    if event(t):
                        event_list.pop(0)
                    else:
                        break

    def addIdleEvent(self, event):
        self.event_list.append(event)

    def removeIdleEvent(self, event):
        try:
            self.event_list.remove(event)
        except ValueError:
            pass

    def addReader(self, s):
        self.reader_set.add(s)

    def removeReader(self, s):
        self.read_set.discard(s)

    def addWriter(self, s):
        self.writer_set.add(s)

    def removeWriter(self, s):
        self.writer_set.discard(s)

