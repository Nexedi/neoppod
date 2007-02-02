from threading import Thread

from neo.client.NEOStorage import NEOStorageError, NEOStorageConflictError, \
     NEOStorageNotFoundError
import logging

class ThreadingMixIn:
    """Mix-in class to handle each method in a new thread."""

    def process_method_thread(self, method, kw):
        m = getattr(self, method)
        try:
            r = m(**kw)
        except Exception, e:
            r = e.__class__

        self._return_lock_acquire()
        self.returned_data = r

    def process_method(self, method, **kw):
        """Start a new thread to process the method."""
        # XXX why is it necessary to start a new thread here? -yo
        # XXX it is too heavy to create a new thread every time. -yo
        t = Thread(target = self.process_method_thread,
                   args = (method, kw))
        t.start()
        # wait for thread to be completed, returned value must be
        # under protection of a lock
        try:
            t.join()
            r = self.returned_data
            try:
                if issubclass(r, NEOStorageError):
                    raise r()
                elif issubclass(r, Exception):
                    raise NEOStorageError()
            except TypeError:
                pass
            return r
        finally:
            self._return_lock_release()
