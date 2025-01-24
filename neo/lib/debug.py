#
# Copyright (C) 2010-2019  Nexedi SA
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

import errno, os, signal, socket, sys, traceback
from functools import wraps
from neo import *
import neo

# kill -RTMIN+2 <pid>
#   Dump information to logs.
# kill -RTMIN+3 <pid>
#   Loads (or reloads) neo.debug module.
#   The content is up to you (it's only imported). It can be a breakpoint.

def safe_handler(func):
    def wrapper(sig, frame):
        try:
            func(sig, frame)
        except:
            # Prevent exception from exiting signal handler, so mistakes in
            # "debug" module don't kill process.
            traceback.print_exc()
    return wraps(func)(wrapper)

if six.PY3:
    from importlib.util import find_spec, module_from_spec
    debug_spec = find_spec('neo.debug')
    @safe_handler
    def debugHandler(sig, frame):
        debug_spec.loader.exec_module(module_from_spec(debug_spec))
else:
    import imp
    debug_spec = imp.find_module('debug', neo.__path__)
    @safe_handler
    def debugHandler(sig, frame):
        imp.load_module('neo.debug', *debug_spec)

def getPdb(**kw):
    try: # try ipython if available
        if six.PY3:
            raise ImportError
        import IPython
        shell = IPython.terminal.embed.InteractiveShellEmbed()
        return IPython.core.debugger.Pdb(shell.colors, **kw)
    except (AttributeError, ImportError):
        import pdb
        return pdb.Pdb(**kw)

_debugger = None

def winpdb(depth=0):
    import rpdb2
    depth += 1
    if rpdb2.g_debugger is not None:
        return rpdb2.setbreak(depth)
    script = rpdb2.calc_frame_path(sys._getframe(depth))
    pwd = str(os.getpid()) + os.getcwd().replace('/', '_').replace('-', '_')
    pid = os.fork()
    if pid:
        try:
            rpdb2.start_embedded_debugger(pwd, depth=depth)
        finally:
            os.waitpid(pid, 0)
    else:
        try:
            os.execlp('python', 'python', '-c', """import os\nif not os.fork():
                import rpdb2, winpdb
                rpdb2_raw_input = rpdb2._raw_input
                rpdb2._raw_input = lambda s: \
                    s == rpdb2.STR_PASSWORD_INPUT and %r or rpdb2_raw_input(s)
                winpdb.g_ignored_warnings[winpdb.STR_EMBEDDED_WARNING] = True
                winpdb.main()
            """ % pwd, '-a', script)
        finally:
            os.abort()

def register(on_log=None):
    try:
        if on_log is not None:
            @safe_handler
            def on_log_signal(signum, signal):
                on_log()
            signal.signal(signal.SIGRTMIN+2, on_log_signal)
        signal.signal(signal.SIGRTMIN+3, debugHandler)
    except ValueError: # signal only works in main thread
        pass


class PdbSocket(object):

    def __init__(self, socket):
        # In case that the default timeout is not None.
        socket.settimeout(None)
        self._socket = socket
        self._buf = ''

    def close(self):
        self._socket.close()

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
            try:
                d = recv(4096)
            except socket.error as e:
                if e.errno != errno.EINTR:
                    raise
            else:
                if not d:
                    self._buf = ''
                    return data
                data += bytes2str(d)

    def flush(self):
        pass

    def closed(self):
        self._socket.setblocking(0)
        try:
            self._socket.recv(0)
            return True
        except socket.error as e:
            if e.errno != errno.EAGAIN:
                raise
            self._socket.setblocking(1)
            return False
