import os
import sys
import threading
import traceback
from collections import deque
from time import time
from Queue import Empty

"""
  Verbose locking classes.

  Python threading module contains a simple logging mechanism, but:
    - It's limitted to RLock class
    - It's enabled instance by instance
    - Choice to log or not is done at instanciation
    - It does not emit any log before trying to acquire lock

  This file defines a VerboseLock class implementing basic lock API and
  logging in appropriate places with extensive details.

  It can be globaly toggled by changing VERBOSE_LOCKING value.
  There is no overhead at all when disabled (passthrough to threading
  classes).
"""

VERBOSE_LOCKING = False


class LockUser(object):

    def __init__(self, message, level=0):
        t = threading.currentThread()
        ident = getattr(t, 'node_name', t.name)
        # This class is instanciated from a place desiring to known what
        # called it.
        # limit=1 would return execution position in this method
        # limit=2 would return execution position in caller
        # limit=3 returns execution position in caller's caller
        # Additionnal level value (should be positive only) can be used when
        # more intermediate calls are involved
        self.stack = stack = traceback.extract_stack()[:-2-level]
        path, line_number, func_name, line = stack[-1]
        # Simplify path. Only keep 3 last path elements. It is enough for
        # current Neo directory structure.
        path = os.path.join('...', *path.split(os.path.sep)[-3:])
        self.time = time()
        self.ident = "%s@%r %s:%s %s" % (
            ident, self.time, path, line_number, line)
        self.note(message)
        self.ident = ident

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.ident == other.ident

    def __repr__(self):
        return "%s@%r" % (self.ident, self.time)

    def formatStack(self):
        return ''.join(traceback.format_list(self.stack))

    def note():
        write = sys.stderr.write
        flush = sys.stderr.flush
        def note(self, message):
            write("[%s] %s\n" % (self.ident, message))
            flush()
        return note
    note = note()


class VerboseLockBase(object):

    _error_class = threading.ThreadError
    _release_error = 'release unlocked lock'

    def __init__(self, check_owner, name=None, verbose=None):
        self._check_owner = check_owner
        self._name = name or '<%s@%X>' % (self.__class__.__name__, id(self))
        self.owner = None
        self.waiting = []
        LockUser(repr(self) + " created", 1)

    def acquire(self, blocking=1):
        owner = self.owner if self._locked() else None
        me = LockUser("%s.acquire(%s). Owned by %r. Waiting: %r"
                      % (self, blocking, owner, self.waiting))
        if blocking:
            if self._check_owner and me == owner:
                me.note("I already own this lock: %r" % owner)
                me.note("Owner traceback:\n%s" % owner.formatStack())
                me.note("My traceback:\n%s" % me.formatStack())
            self.waiting.append(me)
        try:
            locked = self.lock.acquire(blocking)
        finally:
            if blocking:
                self.waiting.remove(me)
        if locked:
            self.owner = me
            me.note("Lock granted. Waiting: " + repr(self.waiting))
        return locked

    __enter__ = acquire

    def release(self):
        me = LockUser("%s.release(). Waiting: %r" % (self, self.waiting))
        try:
            return self.lock.release()
        except self._error_class:
            t, v, tb = sys.exc_info()
            if str(v) == self._release_error:
                raise t, "%s %s (%s)" % (v, self, me), tb
            raise

    def __exit__(self, t, v, tb):
        self.release()

    def _locked(self):
        raise NotImplementedError

    def __repr__(self):
        return self._name


class VerboseRLock(VerboseLockBase):

    _error_class = RuntimeError
    _release_error = 'cannot release un-acquired lock'

    def __init__(self, **kw):
        super(VerboseRLock, self).__init__(check_owner=False, **kw)
        self.lock = threading.RLock()

    def _locked(self):
        return self.lock._RLock__block.locked()

    def _is_owned(self):
        return self.lock._is_owned()

class VerboseLock(VerboseLockBase):

    def __init__(self, check_owner=True, **kw):
        super(VerboseLock, self).__init__(check_owner, **kw)
        self.lock = threading.Lock()

    def locked(self):
        return self.lock.locked()
    _locked = locked

class VerboseSemaphore(VerboseLockBase):

    def __init__(self, value=1, check_owner=True, **kw):
        super(VerboseSemaphore, self).__init__(check_owner, **kw)
        self.lock = threading.Semaphore(value)

    def _locked(self):
        return not self.lock._Semaphore__value


if VERBOSE_LOCKING:
    Lock = VerboseLock
    RLock = VerboseRLock
    Semaphore = VerboseSemaphore
else:
    Lock = threading.Lock
    RLock = threading.RLock
    Semaphore = threading.Semaphore


class SimpleQueue(object):
    """
    Similar to Queue.Queue but with simpler locking scheme, reducing lock
    contention on "put" (benchmark shows 60% less time spent in "put").
    As a result:
    - only a single consumer possible ("get" vs. "get" race condition)
    - only a single producer possible ("put" vs. "put" race condition)
    - no blocking size limit possible
    - no consumer -> producer notifications (task_done/join API)

    Queue is on the critical path: any moment spent here increases client
    application wait for object data, transaction completion, etc.
    As we have a single consumer (client application's thread) and a single
    producer (lib.dispatcher, which can be called from several threads but
    serialises calls internally) for each queue, Queue.Queue's locking scheme
    can be relaxed to reduce latency.
    """
    __slots__ = ('_lock', '_unlock', '_popleft', '_append', '_queue')
    def __init__(self):
        lock = Lock()
        self._lock = lock.acquire
        self._unlock = lock.release
        self._queue = queue = deque()
        self._popleft = queue.popleft
        self._append = queue.append

    def get(self, block):
        if block:
            self._lock(False)
        while True:
            try:
                return self._popleft()
            except IndexError:
                if not block:
                    raise Empty
                self._lock()

    def put(self, item):
        self._append(item)
        self._lock(False)
        self._unlock()

    def empty(self):
        return not self._queue
