#
# Copyright (C) 2006-2019  Nexedi SA
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

class NeoException(Exception):
    pass

class PrimaryElected(NeoException):
    pass

class PrimaryFailure(NeoException):
    pass

class StoppedOperation(NeoException):
    pass

class NodeNotReady(NeoException):
    pass

class ProtocolError(NeoException):
    """ Base class for protocol errors, close the connection """

class PacketMalformedError(ProtocolError):
    pass

class UnexpectedPacketError(ProtocolError):
    pass

class NotReadyError(ProtocolError):
    pass

class BackendNotImplemented(NeoException):
    """ Method not implemented by backend storage """

class NonReadableCell(NeoException):
    """Read-access to a cell that is actually non-readable

    This happens in case of race condition at processing partition table
    updates: client's PT is older or newer than storage's. The latter case is
    possible because the master must validate any end of replication, which
    means that the storage node can't anticipate the PT update (concurrently,
    there may be a first tweaks that moves the replicated cell to another node,
    and a second one that moves it back).

    On such event, the client must retry, preferably another cell.
    """
