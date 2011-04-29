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

import neo.lib.python
import logging as logging_std

FMT = ('%(asctime)s %(levelname)-9s %(name)-10s'
       ' [%(module)14s:%(lineno)3d] \n%(message)s')

class Formatter(logging_std.Formatter):

    def formatTime(self, record, datefmt=None):
        return logging_std.Formatter.formatTime(self, record,
           '%Y-%m-%d %H:%M:%S') + '.%04d' % (record.msecs * 10)

    def format(self, record):
        lines = iter(logging_std.Formatter.format(self, record).splitlines())
        prefix = lines.next()
        return '\n'.join(prefix + line for line in lines)

def setupLog(name='NEO', filename=None, verbose=False):
    global logging
    if verbose:
        level = logging_std.DEBUG
    else:
        level = logging_std.INFO
    if logging is not None:
        for handler in logging.handlers:
            handler.acquire()
            try:
                handler.close()
            finally:
                handler.release()
        del logging.manager.loggerDict[logging.name]
    logging = logging_std.getLogger(name)
    for handler in logging.handlers[:]:
        logging.removeHandler(handler)
    if filename is None:
        handler = logging_std.StreamHandler()
    else:
        handler = logging_std.FileHandler(filename)
    handler.setFormatter(Formatter(FMT))
    logging.setLevel(level)
    logging.addHandler(handler)
    logging.propagate = 0

# Create default logger
logging = None
setupLog()

