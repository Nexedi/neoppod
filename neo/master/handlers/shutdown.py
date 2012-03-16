#
# Copyright (C) 2006-2012  Nexedi SA

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

import neo.lib
from neo.lib import protocol
from . import BaseServiceHandler

def reject(*args, **kw):
    raise protocol.ProtocolError('cluster is shutting down')

class ShutdownHandler(BaseServiceHandler):
    """This class deals with events for a shutting down phase."""

    requestIdentification = reject
    askBeginTransaction = reject
