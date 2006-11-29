import logging
from select import select
from time import time

class IdleEvent(object):
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
            logging.info('timeout with %s:%d', *(conn.getAddress()))
            conn.getHandler().timeoutExpired(conn)
            conn.close()
            return True
        elif t > self._time:
            if self._additional_timeout > 5:
                self._additional_timeout -= 5
                conn.expectMessage(self._id, 5, self._additional_timeout)
                # Start a keep-alive packet.
                logging.info('sending a ping to %s:%d', *(conn.getAddress()))
                msg_id = conn.getNextId()
                conn.addPacket(Packet().ping(msg_id))
                conn.expectMessage(msg_id, 5, 0)
            else:
                conn.expectMessage(self._id, self._additional_timeout, 0)
            return True
        return False

class EventManager(object):
    """This class manages connections and events."""

    def __init__(self):
        self.connection_dict = {}
        self.reader_set = set([])
        self.writer_set = set([])
        self.exc_list = []
        self.event_list = []
        self.prev_time = time()

    def getConnectionList(self):
        return self.connection_dict.values()

    def register(self, conn):
        self.connection_dict[conn.getSocket()] = conn

    def unregister(self, conn):
        del self.connection_dict[conn.getSocket()]

    def poll(self, timeout = 1):
        rlist, wlist, xlist = select(self.reader_set, self.writer_set, self.exc_list,
                                     timeout)
        for s in rlist:
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
                event_list.sort(key = lambda event: event.getTime())
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

    def addReader(self, conn):
        self.reader_set.add(conn.getSocket())

    def removeReader(self, conn):
        self.reader_set.discard(conn.getSocket())

    def addWriter(self, conn):
        self.writer_set.add(conn.getSocket())

    def removeWriter(self, conn):
        self.writer_set.discard(conn.getSocket())

