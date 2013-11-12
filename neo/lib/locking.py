from threading import Lock as threading_Lock
from threading import RLock as threading_RLock
from threading import currentThread
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

__all__ = ['Lock', 'RLock', 'Queue', 'Empty']

VERBOSE_LOCKING = False

import traceback
import sys
import os

class LockUser(object):
    def __init__(self, level=0):
        self.ident = currentThread().getName()
        # This class is instanciated from a place desiring to known what
        # called it.
        # limit=1 would return execution position in this method
        # limit=2 would return execution position in caller
        # limit=3 returns execution position in caller's caller
        # Additionnal level value (should be positive only) can be used when
        # more intermediate calls are involved
        self.stack = stack = traceback.extract_stack()[:-(2 + level)]
        path, line_number, func_name, line = stack[-1]
        # Simplify path. Only keep 3 last path elements. It is enough for
        # current Neo directory structure.
        path = os.path.join('...', *path.split(os.path.sep)[-3:])
        self.caller = (path, line_number, func_name, line)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.ident == other.ident

    def __repr__(self):
        return '%s@%s:%s %s' % (self.ident, self.caller[0], self.caller[1],
                self.caller[3])

    def formatStack(self):
        return ''.join(traceback.format_list(self.stack))

class VerboseLockBase(object):
    def __init__(self, reentrant=False, debug_lock=False):
        self.reentrant = reentrant
        self.debug_lock = debug_lock
        self.owner = None
        self.waiting = []
        self._note('%s@%X created by %r', self.__class__.__name__, id(self),
                LockUser(1))

    def _note(self, fmt, *args):
        sys.stderr.write(fmt % args + '\n')
        sys.stderr.flush()

    def _getOwner(self):
        if self._locked():
            owner = self.owner
        else:
            owner = None
        return owner

    def acquire(self, blocking=1):
        me = LockUser()
        owner = self._getOwner()
        self._note('[%r]%s.acquire(%s) Waiting for lock. Owned by:%r ' \
                'Waiting:%r', me, self, blocking, owner, self.waiting)
        if (self.debug_lock and owner is not None) or  \
                (not self.reentrant and blocking and me == owner):
            if me == owner:
                self._note('[%r]%s.acquire(%s): Deadlock detected: ' \
                    ' I already own this lock:%r', me, self, blocking, owner)
            else:
                self._note('[%r]%s.acquire(%s): debug lock triggered: %r',
                        me, self, blocking, owner)
            self._note('Owner traceback:\n%s', owner.formatStack())
            self._note('My traceback:\n%s', me.formatStack())
        self.waiting.append(me)
        try:
            return self.lock.acquire(blocking)
        finally:
            self.owner = me
            self.waiting.remove(me)
            self._note('[%r]%s.acquire(%s) Lock granted. Waiting: %r',
                    me, self, blocking, self.waiting)

    __enter__ = acquire

    def release(self):
        me = LockUser()
        self._note('[%r]%s.release() Waiting: %r', me, self, self.waiting)
        return self.lock.release()

    def __exit__(self, t, v, tb):
        self.release()

    def _locked(self):
        raise NotImplementedError

    def __repr__(self):
        return '<%s@%X>' % (self.__class__.__name__, id(self))

class VerboseRLock(VerboseLockBase):
    def __init__(self, verbose=None, debug_lock=False):
        super(VerboseRLock, self).__init__(reentrant=True,
                debug_lock=debug_lock)
        self.lock = threading_RLock()

    def _locked(self):
        return self.lock._RLock__block.locked()

    def _is_owned(self):
        return self.lock._is_owned()

class VerboseLock(VerboseLockBase):
    def __init__(self, verbose=None, debug_lock=False):
        super(VerboseLock, self).__init__(debug_lock=debug_lock)
        self.lock = threading_Lock()

    def locked(self):
        return self.lock.locked()
    _locked = locked

if VERBOSE_LOCKING:
    Lock = VerboseLock
    RLock = VerboseRLock
else:
    Lock = threading_Lock
    RLock = threading_RLock
