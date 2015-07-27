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
    import socket, sys, threading
    from neo.lib.debug import getPdb
    #from pdb import Pdb as getPdb

    class Socket(object):

        def __init__(self, socket):
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

    def pdb(app_set):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(('127.0.0.1', 0))
            s.listen(0)
            print 'Listening to %u' % s.getsockname()[1]
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
        app_set = sys.modules['neo.client.app'].app_set
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
    threading.Thread(target=pdb, args=(app_set,)).start()

elif IF == 'frames':
    import sys, traceback
    write = sys.stderr.write
    for thread_id, frame in sys._current_frames().iteritems():
        write("Thread %s:\n" % thread_id)
        traceback.print_stack(frame)
    write("End of dump\n")
