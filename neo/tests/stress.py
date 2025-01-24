# tools/stress is split in such a way that this file can be reused to
# implement another tool to stress an existing cluster, which would be filled
# by a real application.

# XXX: Killing storage nodes out of the control of the master
#      (tools/stress -r RATIO with RATIO != 0) sometimes causes the cluster to
#      become non-operational. The race condition is that by the time the
#      master notices a node is killed, it may finish a transaction for which
#      the client node got failures with the replica, which gets disconnected.
#      The code tries to anticipate failures, by assuming a node is down until
#      it is actually down and again running, but even after a single kill,
#      long-running commits (e.g deadlock resolution) can cause several
#      failures+resync in a row for the same node.
#      The only solution looks like to not abort if it goes to RECOVERY.

import curses, os, random, re, select, threading, time
from collections import deque
from neo import *
from neo.lib import logging, protocol
from neo.lib.app import BaseApplication
from neo.lib.debug import register as registerLiveDebugger
from neo.lib.exception import PrimaryFailure
from neo.lib.protocol import ClusterStates, NodeStates, NodeTypes, Packets, \
    uuid_str
from neo.admin.app import Application as AdminApplication
from neo.admin.handler import MasterEventHandler


class Handler(MasterEventHandler):

    def answerClusterState(self, conn, state):
        super(Handler, self).answerClusterState(conn, state)
        self.app.refresh('state')

    def sendPartitionTable(self, *args):
        super(Handler, self).sendPartitionTable(*args)
        self.app.refresh('pt')

    def notifyPartitionChanges(self, *args):
        super(Handler, self).notifyPartitionChanges(*args)
        self.app.refresh('pt')

    def answerLastIDs(self, conn, *args):
        self.app.answerLastIDs(*args)

    def notifyNodeInformation(self, conn, timestamp, node_list):
        for node_type, addr, uuid, state, id_timestamp in node_list:
            if node_type == NodeTypes.CLIENT and state == NodeStates.UNKNOWN:
                self.app.clientDown()
                break
        getStorageList = self.app.nm.getStorageList
        before = [node for node in getStorageList() if node.isRunning()]
        super(Handler, self).notifyNodeInformation(conn, timestamp, node_list)
        self.app.notifyNodeInformation(
            {node for node in getStorageList() if node.isRunning()}
            .difference(before))


class StressApplication(AdminApplication):

    backup_dict = {}
    cluster_state = server = uuid = None
    listening_conn = True
    fault_probability = 1
    restart_ratio = float('inf') # no firewall support
    _stress = False

    def __init__(self, ssl_credentials, master_nodes):
        BaseApplication.__init__(self, ssl_credentials)
        self.nm.createMasters(master_nodes)
        self.pt = None
        self.master_event_handler = Handler(self)
        self.reset()
        registerLiveDebugger(on_log=self.log)
        self.failing = set()
        self.restart_lock = threading.Lock()

    def close(self):
        BaseApplication.close(self)

    def updateMonitorInformation(*args, **kw):
        pass

    def run(self):
        visibility = None
        from logging import disable, ERROR
        disable(ERROR)
        self.stdscr = curses.initscr()
        try:
            curses.noecho()
            curses.cbreak()
            self.stdscr.keypad(1)
            visibility = curses.curs_set(0)
            self._run()
        finally:
            if visibility:
                curses.curs_set(visibility)
            self.stdscr.keypad(0)
            curses.echo()
            curses.nocbreak()
            curses.endwin()

    def _run(self):
        stdscr = self.stdscr
        r, w = os.pipe()
        l = threading.Lock()
        stdscr.nodelay(1)
        input_queue = deque()
        def input_read():
            x = []
            while 1:
                c = stdscr.getch()
                if c < 0:
                    if x:
                        input_queue.append(x)
                    return input_queue
                x.append(c)
        def input_thread():
            try:
                poll = select.poll()
                poll.register(0, select.POLLIN)
                poll.register(r, select.POLLIN)
                while 1:
                    for fd, _ in poll.poll():
                        if fd:
                            return
                        with l:
                            empty = not input_queue
                            if input_read() and empty:
                                self.em.wakeup()
            finally:
                os.close(r)
        t = threading.Thread(target=input_thread)
        t.deamon = True
        wait = None
        try:
            t.start()
            self.startCluster()
            self.refresh('stress', False)
            while 1:
                self.failing.clear()
                try:
                    self.connectToPrimary()
                    self.askLastIDs()
                    while 1:
                        self.em.poll(1)
                        with l:
                            if input_read():
                                for x in input_queue:
                                    try:
                                        x, = x
                                    except ValueError:
                                        continue
                                    if x == curses.KEY_RESIZE:
                                        self.refresh()
                                    elif x == curses.KEY_F1:
                                        self.stress()
                                    else:
                                        try:
                                            x = chr(x)
                                        except ValueError:
                                            continue
                                        if x == 'q':
                                            return
                                input_queue.clear()
                except PrimaryFailure:
                    logging.error('primary master is down')
                    if self.cluster_state == ClusterStates.STOPPING:
                        break
                    self.primaryFailure()
                finally:
                    if self._stress:
                        self.stress()
            wait = time.time()
        finally:
            os.write(w, b'\0')
            os.close(w)
            t.join()
            self.stopCluster(wait)

    def primaryFailure(self):
        raise

    def startCluster(self):
        raise NotImplementedError

    def stopCluster(self, wait):
        raise NotImplementedError

    def clientDown(self):
        send = self.master_conn.send
        send(Packets.FlushLog())
        send(Packets.SetClusterState(ClusterStates.STOPPING))

    def notifyNodeInformation(self, node_list):
        for node in node_list:
            self.failing.discard(node.getUUID())

    def askLastIDs(self):
        conn = self.master_conn
        if conn:
            conn.ask(Packets.AskLastIDs())

    def answerLastIDs(self, ltid, loid, ftid):
        self.loid = loid
        self.ltid = ltid
        self.em.setTimeout(int(time.time() + 1), self.askLastIDs)
        if self._stress and random.random() < self.fault_probability:
            node_list = self.nm.getStorageList()
            random.shuffle(node_list)
            fw = []
            kill = []
            restart_ratio = self.restart_ratio
            for node in node_list:
                nid = node.getUUID()
                if nid in self.failing:
                    if restart_ratio <= 1:
                        fw.append(nid)
                    continue
                running = node.isRunning()
                if running or restart_ratio <= 1:
                    self.failing.add(nid)
                    if self.pt.operational(self.failing):
                        (kill if running and random.random() < restart_ratio
                              else fw).append(nid)
                        if len(self.failing) == self._fault_count:
                            break
                    else:
                        self.failing.remove(nid)
            if fw or kill:
                logging.info('stress(fw=(%s), kill=(%s))',
                    ','.join(map(uuid_str, fw)),
                    ','.join(map(uuid_str, kill)))
                for nid in fw:
                    self.tcpReset(nid)
                if kill:
                    t = threading.Thread(target=self._restart, args=kill)
                    t.daemon = 1
                    t.start()
                self.refresh('pt', False)
        self.refresh('ids')

    def _restart(self, *nids):
        with self.restart_lock:
            self.restartStorages(nids)

    def tcpReset(self, nid):
        raise NotImplementedError

    def restartStorages(self, nids):
        raise NotImplementedError

    def refresh(self, what=None, do=True):
        stdscr = self.stdscr
        try:
            y = 0
            if what in (None, 'stress'):
                stdscr.addstr(y, 0, 'stress: %s (toggle with F1)\n'
                    % ('yes' if self._stress else 'no'))
            y += 1
            if what in (None, 'state'):
                stdscr.addstr(y, 0, 'cluster state: %s\n' % self.cluster_state)
            y += 1
            if what in (None, 'ids'):
                self.refresh_ids(y)
                h = stdscr.getyx()[0] - y
                clear = self._ids_height - h
                if clear:
                    self._ids_height = h
                    what = None
            else:
                clear = None
            y += self._ids_height
            if what in (None, 'pt'):
                pt = self.pt
                n = len(str(pt.np-1))
                node_list = sorted(pt.count_dict)
                attr = curses.A_NORMAL, curses.A_BOLD
                stdscr.addstr(y, 0, 'pt id: %s\n  %s' % (pt.getID(), ' ' * n))
                for node in node_list:
                    stdscr.addstr(
                        protocol.node_state_prefix_dict[node.getState()],
                        attr[node.getUUID() in self.failing])
                stdscr.addstr('\n')
                x = '%{}s'.format(n)
                n = pt.nr + 1
                split = re.compile('[^OC]+|[OC]+').findall
                for i, r in enumerate(pt._formatRows(node_list)):
                    stdscr.addstr(x % i, attr[r.count('U') != n])
                    for i, r in enumerate(split(': %s\n' % r)):
                        stdscr.addstr(r, attr[i & 1])
                if clear:
                    stdscr.addstr('\n' * clear)
        except curses.error:
            pass
        if do:
            stdscr.refresh()

    # _ids_height

    def refresh_ids(self, y):
      raise NotImplementedError

    def stress(self):
        self._stress = not self._stress
        self.refresh('stress')
