from threading import Lock as threading_Lock
from threading import RLock as threading_RLock
from threading import currentThread
import traceback
import sys
import os

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

__all__ = ['Lock', 'RLock']

VERBOSE_LOCKING = False

if VERBOSE_LOCKING:
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
            path, line_number, func_name, line = traceback.extract_stack(limit=3 + level)[0]
            # Simplify path. Only keep 3 last path elements. It is enough for
            # current Neo directory structure.
            path = os.path.join('...', *path.split(os.path.sep)[-3:])
            self.caller = (path, line_number, func_name, line)

        def __eq__(self, other):
            return isinstance(other, self.__class__) and self.ident == other.ident

        def __repr__(self):
            return '%s@%s:%s %s' % (self.ident, self.caller[0], self.caller[1], self.caller[3])

    class VerboseLock(object):
        def __init__(self):
            self.owner = None
            self.waiting = []
            self._note('%s@%X created by %r', self.__class__.__name__, id(self), LockUser(1))

        def _note(self, format, *args):
            sys.stderr.write(format % args + '\n')
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
            self._note('[%r]%s.acquire(%s) Waiting for lock. Owned by:%r Waiting:%r', me, self, blocking, owner, self.waiting)
            if blocking and me == owner:
                self._note('[%r]%s.acquire(%s): Deadlock detected: I already own this lock:%r', me, self, blocking, owner)
            self.waiting.append(me)
            try:
                return self.lock.acquire(blocking)
            finally:
                self.owner = me
                self.waiting.remove(me)
                self._note('[%r]%s.acquire(%s) Lock granted. Waiting: %r', me, self, blocking, self.waiting)

        def release(self):
            me = LockUser()
            self._note('[%r]%s.release() Waiting: %r', me, self, self.waiting)
            return self.lock.release()

        def _locked(self):
            raise NotImplementedError

        def __repr__(self):
            return '<%s@%X>' % (self.__class__.__name__, id(self))

    class RLock(VerboseLock):
        def __init__(self, verbose=None):
            VerboseLock.__init__(self)
            self.lock = threading_RLock()

        def _locked(self):
            return self.lock.__block.locked()

        def _is_owned(self):
            return self.lock._is_owned()

    class Lock(VerboseLock):
        def __init__(self, verbose=None):
            VerboseLock.__init__(self)
            self.lock = threading_Lock()

        def locked(self):
            return self.lock.locked()
        _locked = locked
else:
    Lock = threading_Lock
    RLock = threading_RLock

