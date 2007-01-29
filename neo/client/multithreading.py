from threading import Thread

class ThreadingMixIn:
    """Mix-in class to handle each method in a new thread."""

    def process_method_thread(self, method, kw):
        m = getattr(self, method)
        try:
            r = m(**kw)
        finally:
            self._return_lock_acquire()
            self.returned_data = r

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
