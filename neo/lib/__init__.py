#
# Copyright (C) 2006-2010  Nexedi SA
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

import logging as logging_std

PREFIX = '%(asctime)s %(levelname)-9s %(name)-10s'
SUFFIX = ' [%(module)14s:%(lineno)3d] %(message)s'

def setupLog(name='NEO', filename=None, verbose=False):
    global logging
    if verbose:
        level = logging_std.DEBUG
    else:
        level = logging_std.INFO
    fmt = PREFIX + SUFFIX
    logging = logging_std.getLogger(name.upper())
    for handler in logging.handlers[:]:
        logging.removeHandler(handler)
    if filename is None:
        handler = logging_std.StreamHandler()
    else:
        handler = logging_std.FileHandler(filename)
    handler.setFormatter(logging_std.Formatter(fmt))
    logging.setLevel(level)
    logging.addHandler(handler)
    logging.propagate = 0

# Create default logger
setupLog()

