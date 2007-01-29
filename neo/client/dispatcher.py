from threading import Thread
from Queue import Empty

from neo.protocol import PING, Packet

class Dispatcher(Thread):
    """Dispatcher class use to redirect request to thread."""

    def __init__(self, em, message_queue, request_queue, **kw):
        Thread.__init__(self, **kw)
        self._message_queue = message_queue
        self._request_queue = request_queue
        self.em = em
        # This dict is used to associate conn/message id to client thread queue
        # and thus redispatch answer to the original thread
        self.message_table = {}

    def run(self):
        while 1:
            # First check if we receive any new message from other node
            self.message = None
            m = None
            self.em.poll(1)
            if self.message is not None:
                conn, packet = self.message
                # now send message to waiting thread
                key = "%s-%s" %(conn.getUUID(),packet.getId())
                if self.message_table.has_key(key):
                    tmp_q = self.message_table.pop(key)
                    tmp_q.put(self.message, True)
                else:
                    conn, packet = self.message
                    method_type = packet.getType()
                    if method_type == PING:
                        # must answer with no delay
                        conn.addPacket(Packet().pong(packet.getId()))
                    else:
                        # put message in request queue
                        self._request_queue.put(self.message, True)

            # Then check if a client ask me to send a message
            try:
                m = self._message_queue.get_nowait()
                if m is not None:
                    tmp_q, msg_id, conn, p = m
                    conn.addPacket(p)
                    conn.expectMessage(msg_id)
                    if tmp_q is not None:
                        key = "%s-%s" %(conn.getUUID(),msg_id)
                        self.message_table[key] = tmp_q
            except Empty:
                continue



