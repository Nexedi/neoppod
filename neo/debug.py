"""Example of script starting a debugger on RTMIN+3 signal

The pdb is launched in a separate thread in order not to trigger timeouts.
The prompt is accessible through network in case that the process is daemonized:
  $ socat READLINE TCP:127.0.0.1:54930
  > neo/debug.py(63)pdb()
  -> app # this is Application instance
  (Pdb) app
  <neo.master.app.Application object at 0x1fc9750>
"""

IF = 'pdb'
if IF == 'pdb':
    # List of (module, callables) to break at.
    # If empty, a thread is started with a breakpoint.
    # All breakpoints are automatically removed on the first break,
    # or when this module is reloaded.
    BP = (#('ZODB.Connection', 'Connection.setstate'),
          #('ZPublisher.Publish', 'publish_module_standard'),
         )

    import errno, socket, sys, threading, weakref
    # Unfortunately, IPython does not always print to given stdout.
    #from neo.lib.debug import getPdb
    from pdb import Pdb as getPdb

    class Socket(object):

        def __init__(self, socket):
            # In case that the default timeout is not None.
            socket.settimeout(None)
            self._socket = socket
            self._buf = ''

        def write(self, data):
            self._socket.send(data)

        def readline(self):
            recv = self._socket.recv
            data = self._buf
            while True:
                i = 1 + data.find('\n')
                if i:
                    self._buf = data[i:]
                    return data[:i]
                d = recv(4096)
                data += d
                if not d:
                    self._buf = ''
                    return data

        def flush(self):
            pass

        def closed(self):
            self._socket.setblocking(0)
            try:
                self._socket.recv(0)
                return True
            except socket.error, (err, _):
                if err != errno.EAGAIN:
                    raise
                self._socket.setblocking(1)
            return False

    def pdb():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # For better security, maybe we should use a unix socket.
            s.settimeout(60)
            s.bind(('127.0.0.1', 0))
            s.listen(0)
            print 'Listening to %u' % s.getsockname()[1]
            sys.stdout.flush() # BBB: On Python 3, print() takes a 'flush' arg.
            _socket = Socket(s.accept()[0])
        finally:
            s.close()
        try:
            app, = app_set
        except ValueError:
            app = None
        getPdb(stdin=_socket, stdout=_socket).set_trace()
        app # this is Application instance (see 'app_set' if there are several)

    try:
        app_set = sys.modules['neo.lib.threaded_app'].app_set
    except KeyError:
        f = sys._getframe(3)
        try:
            while f.f_code.co_name != 'run' or \
                  f.f_locals.get('self').__class__.__name__ != 'Application':
                f = f.f_back
            app_set = f.f_locals['self'],
        except AttributeError:
            app_set = ()
        finally:
            del f

    class setupBreakPoints(list):

        def __init__(self, bp_list):
            self._lock = threading.Lock()
            for o, name in bp_list:
                o = __import__(o, fromlist=1)
                x = name.split('.')
                name = x.pop()
                for x in x:
                    o = getattr(o, x)
                orig = getattr(o, name)
                if orig.__module__ == __name__:
                    orig.__closure__[1].cell_contents._revert()
                    orig = getattr(o, name)
                    assert orig.__module__ != __name__, (o, name)
                orig = getattr(orig, '__func__', orig)
                self.append((o, name, orig))
                setattr(o, name, self._wrap(orig))
                print 'BP set on', orig
            sys.stdout.flush()
            self._hold = weakref.ref(pdb, self._revert)

        def _revert(self, *_):
            for x in self:
                setattr(*x)
                print 'BP removed on', x[2]
            sys.stdout.flush()
            del self[:]

        def _wrap(self, orig):
            return lambda *args, **kw: self(orig, *args, **kw)

        def __call__(self, orig, *args, **kw):
            stop = False
            with self._lock:
                if self:
                    stop = True
                    self._revert()
            if stop:
                pdb()
            return orig(*args, **kw)

    if BP:
        setupBreakPoints(BP)
    else:
        threading.Thread(target=pdb).start()

elif IF == 'frames':
    import sys, traceback
    write = sys.stderr.write
    for thread_id, frame in sys._current_frames().iteritems():
        write("Thread %s:\n" % thread_id)
        traceback.print_stack(frame)
    write("End of dump\n")
