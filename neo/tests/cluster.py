#
# Copyright (C) 2011-2015  Nexedi SA
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

import __builtin__
import errno
import mmap
import os
import psutil
import signal
import sys
import tempfile
from cPickle import dumps, loads
from functools import wraps
from time import time, sleep
from neo.lib import debug


class ClusterDict(dict):
    """Simple storage (dict), shared with forked processes"""

    _acquired = 0

    def __init__(self, *args, **kw):
        dict.__init__(self, *args, **kw)
        self._r, self._w = os.pipe()
        # shm_open(3) would be better but Python doesn't provide it.
        # See also http://nikitathespider.com/python/shm/
        with tempfile.TemporaryFile() as f:
            f.write(dumps(self.copy(), -1))
            f.flush()
            self._shared = mmap.mmap(f.fileno(), f.tell())
        self.release()

    def __del__(self):
        try:
            os.close(self._r)
            os.close(self._w)
        except TypeError: # if os.close is None
            pass

    def acquire(self):
        self._acquired += 1
        if not self._acquired:
            os.read(self._r, 1)
            try:
                self.clear()
                shared = self._shared
                shared.resize(shared.size())
                self.update(loads(shared[:]))
            except:
                self.release()
                raise

    def release(self, commit=False):
        if not self._acquired:
            if commit:
                self.commit()
            os.write(self._w, '\0')
        self._acquired -= 1

    def commit(self):
        shared = self._shared
        p = dumps(self.copy(), -1)
        shared.resize(len(p))
        shared[:] = p

cluster_dict = ClusterDict()


class ClusterPdb(object):
    """Multiprocess-aware wrapper around console and winpdb debuggers

    __call__ is the method to break.

    TODO: monkey-patch normal code not to timeout
          if another node is being debugged
    """

    def __init__(self):
        self._count_dict = {}

    def __setattr__(self, name, value):
        try:
            hook = getattr(self, name)
            setattr(value.im_self, value.__name__, wraps(value)(
                lambda *args, **kw: hook(value, *args, **kw)))
        except AttributeError:
            object.__setattr__(self, name, value)

    @property
    def broken_peer(self):
        return self._getLastPdb(os.getpid()) is None

    def __call__(self, max_count=None, depth=0, text=None):
        depth += 1
        if max_count:
            frame = sys._getframe(depth)
            key = id(frame.f_code), frame.f_lineno
            del frame
            self._count_dict[key] = count = 1 + self._count_dict.get(key, 0)
            if max_count < count:
                return
        if not text:
            try:
                import rpdb2
            except ImportError:
                if text is not None:
                    raise
            else:
                if rpdb2.g_debugger is None:
                    rpdb2_CStateManager = rpdb2.CStateManager
                    def CStateManager(*args, **kw):
                        rpdb2.CStateManager = rpdb2_CStateManager
                        state_manager = rpdb2.CStateManager(*args, **kw)
                        self._rpdb2_set_state = state_manager.set_state
                        return state_manager
                    rpdb2.CStateManager = CStateManager
                return debug.winpdb(depth)
        try:
            debugger = self.__dict__['_debugger']
        except KeyError:
            assert 'rpdb2' not in sys.modules
            self._debugger = debugger = debug.getPdb()
            self._bdb_interaction = debugger.interaction
        return debugger.set_trace(sys._getframe(depth))

    def kill(self, pid, sig):
        force = []
        sigint_handler = None
        try:
            while 1:
                cluster_dict.acquire()
                try:
                    last_pdb = cluster_dict.get('last_pdb', {})
                    if force or pid not in last_pdb:
                        os.kill(pid, sig)
                        last_pdb.pop(pid, None)
                        cluster_dict.commit()
                        break
                    try:
                        if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                            break
                    except psutil.NoSuchProcess:
                        raise OSError(errno.ESRCH, 'No such process')
                finally:
                    cluster_dict.release()
                if sigint_handler is None:
                    sigint_handler = signal.signal(signal.SIGINT,
                        lambda *args: force.append(None))
                    sys.stderr.write('Pid %u is/was debugged.'
                                    ' Press ^C to kill it...' % pid)
                sleep(1)
        finally:
            if sigint_handler is not None:
                signal.signal(signal.SIGINT, sigint_handler)
        if force:
            sys.stderr.write('\n')

    def _lock_console(self):
        while 1:
            cluster_dict.acquire()
            try:
                if 'text_pdb' not in cluster_dict:
                    cluster_dict['text_pdb'] = pid = os.getpid()
                    cluster_dict.setdefault('last_pdb', {})[pid] = None
                    cluster_dict.commit()
                    break
            finally:
                cluster_dict.release()
            sleep(0.5)

    def _unlock_console(self):
        cluster_dict.acquire()
        try:
            pid = cluster_dict.pop('text_pdb')
            cluster_dict['last_pdb'][pid] = time()
            cluster_dict.commit()
        finally:
            cluster_dict.release()

    def _bdb_interaction(self, hooked, *args, **kw):
        self._lock_console()
        try:
            return hooked(*args, **kw)
        finally:
            self._unlock_console()

    def _rpdb2_set_state(self, hooked, state=None, *args, **kw):
        from rpdb2 import STATE_BROKEN, STATE_DETACHED
        cluster_dict.acquire()
        try:
            if state is None:
                state = hooked.im_self.get_state()
            last_pdb = cluster_dict.setdefault('last_pdb', {})
            pid = os.getpid()
            if state == STATE_DETACHED:
                last_pdb.pop(pid, None)
            else:
                last_pdb[pid] = state != STATE_BROKEN and time() or None
            return hooked(state=state, *args, **kw)
        finally:
            cluster_dict.release(True)

    def _getLastPdb(self, *exclude):
        result = 0
        for pid, last_pdb in cluster_dict.get('last_pdb', {}).iteritems():
            if pid not in exclude:
                if last_pdb is None:
                    return
                if result < last_pdb:
                    result = last_pdb
        return result

    def wait(self, test, timeout):
        end_time = time() + timeout
        period = 0.1
        while not test():
            cluster_dict.acquire()
            try:
                last_pdb = self._getLastPdb()
                if last_pdb is None:
                    next_sleep = 1
                else:
                    next_sleep = max(last_pdb + timeout, end_time) - time()
                    if next_sleep > period:
                        next_sleep = period
                        period *= 1.5
                    elif next_sleep < 0:
                        return False
            finally:
                cluster_dict.release()
            sleep(next_sleep)
        return True

__builtin__.pdb = ClusterPdb()

signal.signal(signal.SIGUSR1, debug.safe_handler(
    lambda sig, frame: pdb(depth=2)))
