"""Example of script starting a debugger on RTMIN+3 signal

The pdb is launched in a separate thread in order not to trigger timeouts.
The prompt is accessible through network in case that the process is daemonized:
  $ socat READLINE TCP:127.0.0.1:54930
  > neo/debug.py(63)pdb()
  -> app # this is Application instance
  (Pdb) app
  <neo.master.app.Application object at 0x1fc9750>
"""

import sys

def app_set():
    try:
        return sys.modules['neo.lib.threaded_app'].app_set
    except KeyError:
        f = sys._getframe(4)
        try:
            while f.f_code.co_name != 'run' or \
                  f.f_locals.get('self').__class__.__name__ != 'Application':
                f = f.f_back
            return f.f_locals['self'],
        except AttributeError:
            return ()

def defer(task):
    def wrapper():
        from traceback import print_exc
        try:
            task(app)
        except:
            print_exc()
    for app in app_set():
        app.em.wakeup(wrapper)
        break

IF = 'pdb'
if IF == 'pdb':
    # List of (module, callables) to break at.
    # If empty, a thread is started with a breakpoint.
    # All breakpoints are automatically removed on the first break,
    # or when this module is reloaded.
    BP = (#('ZODB.Connection', 'Connection.setstate'),
          #('ZPublisher.Publish', 'publish_module_standard'),
         )

    import socket, threading, weakref
    from neo.lib.debug import PdbSocket
    # We don't use the one from neo.lib.debug because unfortunately,
    # IPython does not always print to given stdout.
    from pdb import Pdb as getPdb

    def pdb():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # For better security, maybe we should use a unix socket.
            s.settimeout(60)
            s.bind(('127.0.0.1', 0))
            s.listen(0)
            print 'Listening to %u' % s.getsockname()[1]
            sys.stdout.flush() # BBB: On Python 3, print() takes a 'flush' arg.
            _socket = PdbSocket(s.accept()[0])
        finally:
            s.close()
        try:
            app, = app_set
        except ValueError:
            app = None
        getPdb(stdin=_socket, stdout=_socket).set_trace()
        app # this is Application instance (see 'app_set' if there are several)

    app_set = app_set()

    class setupBreakPoints(list):

        def __init__(self, bp_list):
            self._lock = threading.Lock()
            for o, name in bp_list:
                o = __import__(o, fromlist=('*',), level=0)
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
        threading.Thread(target=pdb, name='pdb').start()

elif IF == 'frames':
    # WARNING: Because of https://bugs.python.org/issue17094, the output is
    #          usually incorrect for subprocesses started by the functional
    #          test framework.
    import traceback
    write = sys.stderr.write
    for thread_id, frame in sys._current_frames().iteritems():
        write("Thread %s:\n" % thread_id)
        traceback.print_stack(frame)
    write("End of dump\n")

elif IF == 'profile':
    DURATION = 60
    def stop(prof, path):
        prof.disable()
        prof.dump_stats(path)
    @defer
    def profile(app):
        import cProfile, threading, time
        from .lib.protocol import uuid_str
        path = 'neo-%s-%s.prof' % (uuid_str(app.uuid), time.time())
        prof = cProfile.Profile()
        threading.Timer(DURATION, stop, (prof, path)).start()
        prof.enable()

elif IF == 'trace-cache':
    from struct import Struct
    from .client.cache import ClientCache
    from .lib.protocol import uuid_str, ZERO_TID as z64

    class CacheTracer(object):

        _pack2 = Struct('!B8s8s').pack
        _pack4 = Struct('!B8s8s8sL').pack

        def __init__(self, cache, path):
            self._cache = cache
            self._trace_file = open(path, 'a')

        def close(self):
            self._trace_file.close()
            return self._cache

        def _trace(self, op, x, y=z64, z=z64):
            self._trace_file.write(self._pack(op, x, y, z))

        def __repr__(self):
            return repr(self._cache)

        @property
        def max_size(self):
            return self._cache.max_size

        def clear(self):
            self._trace_file.write('\0')
            self._cache.clear()

        def clear_current(self):
            self._trace_file.write('\1')
            self._cache.clear_current()

        def load(self, oid, before_tid=None):
            r = self._cache.load(oid, before_tid)
            self._trace_file.write(self._pack2(
                3 if r else 2, oid, before_tid or z64))
            return r

        def store(self, oid, data, tid, next_tid):
            self._trace_file.write(self._pack4(
                4, oid, tid, next_tid or z64, len(data)))
            self._cache.store(oid, data, tid, next_tid)

        def invalidate(self, oid, tid):
            self._trace_file.write(self._pack2(5, oid, tid))
            self._cache.invalidate(oid, tid)

    @defer
    def profile(app):
        app._cache_lock_acquire()
        try:
            cache = app._cache
            if type(cache) is ClientCache:
                app._cache = CacheTracer(cache, '%s-%s.neo-cache-trace' %
                                          (app.name, uuid_str(app.uuid)))
                app._cache.clear()
            else:
                app._cache = cache.close()
        finally:
            app._cache_lock_release()
