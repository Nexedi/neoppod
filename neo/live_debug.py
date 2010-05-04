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
import neo
import pdb

# WARNING: This module should only be used for live application debugging.
# It - by purpose - allows code injection in a running neo process.
# You don't want to enable it in a production environment. Really.
ENABLED = False

# How to include in python code:
#   from neo.live_debug import register
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
    return decorator

@decorate
def debugHandler(sig, frame):
    file, filename, (suffix, mode, type) = imp.find_module('debug',
        neo.__path__)
    imp.load_module('neo.debug', file, filename, (suffix, mode, type))

@decorate
def pdbHandler(sig, frame):
    pdb.set_trace()

def register():
    if ENABLED:
        signal.signal(signal.SIGUSR1, debugHandler)
        signal.signal(signal.SIGUSR2, pdbHandler)

