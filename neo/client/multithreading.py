from threading import Thread

from neo.client.NEOStorage import NEOStorageError, NEOStorageConflictError, \
     NEOStorageNotFoundError, NEO_ERROR, NEO_CONFLICT_ERROR, NEO_NOT_FOUND_ERROR
import logging

class ThreadingMixIn:
    """Mix-in class to handle each method in a new thread."""

    def process_method_thread(self, method, kw):
        m = getattr(self, method)
        r = None
        try:
            r = m(**kw)
            self._return_lock_acquire()
            self.returned_data = r
        except NEOStorageConflictError:
            self._return_lock_acquire()
            self.returned_data = NEO_CONFLICT_ERROR
        except NEOStorageNotFoundError:
            self._return_lock_acquire()
            self.returned_data = NEO_NOT_FOUND_ERROR
        except:
            self._return_lock_acquire()
            self.returned_data = NEO_ERROR


    def process_method(self, method, **kw):
        """Start a new thread to process the method."""
        t = Thread(target = self.process_method_thread,
                   args = (method, kw))
        t.start()
        # wait for thread to be completed, returned value must be
        # under protection of a lock
        try:
            t.join()
            return self.returned_data
        finally:
            self._return_lock_release()
