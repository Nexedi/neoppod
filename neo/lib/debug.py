#
# Copyright (C) 2010  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import traceback
import signal
import ctypes
import imp
import os
import sys
from functools import wraps
import neo

# WARNING: This module should only be used for live application debugging.
# It - by purpose - allows code injection in a running neo process.
# You don't want to enable it in a production environment. Really.
ENABLED = False

# How to include in python code:
#   from neo.debug import register
#   register()
#
# How to trigger it:
#   Kill python process with:
#     SIGUSR1:
#       Loads (or reloads) neo.debug module.
#       The content is up to you (it's only imported).
#     SIGUSR2:
#       Triggers a pdb prompt on process' controlling TTY.

libc = ctypes.cdll.LoadLibrary('libc.so.6')
errno = ctypes.c_int.in_dll(libc, 'errno')

def decorate(func):
    def decorator(sig, frame):
        # Save errno value, to restore it after sig handler returns
        old_errno = errno.value
        try:
            func(sig, frame)
        except:
            # Prevent exception from exiting signal handler, so mistakes in
            # "debug" module don't kill process.
            traceback.print_exc()
        errno.value = old_errno
    return wraps(func)(decorator)

@decorate
def debugHandler(sig, frame):
    file, filename, (suffix, mode, type) = imp.find_module('debug',
        neo.__path__)
    imp.load_module('neo.debug', file, filename, (suffix, mode, type))

def getPdb():
    try: # try ipython if available
        import IPython
        IPython.Shell.IPShell(argv=[])
        return IPython.Debugger.Tracer().debugger
    except ImportError:
        import pdb
        return pdb.Pdb()

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

@decorate
def pdbHandler(sig, frame):
    try:
        winpdb(2) # depth is 2, because of the decorator
    except ImportError:
        global _debugger
        if _debugger is None:
            _debugger = getPdb()
        debugger.set_trace(frame)

def register(on_log=None):
    if ENABLED:
        signal.signal(signal.SIGUSR1, debugHandler)
        signal.signal(signal.SIGUSR2, pdbHandler)
        if on_log is not None:
            # use 'kill -RTMIN <pid>
            @decorate
            def on_log_signal(signum, signal):
                on_log()
            signal.signal(signal.SIGRTMIN, on_log_signal)

