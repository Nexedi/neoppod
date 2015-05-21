#
# Copyright (C) 2006-2015  Nexedi SA
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

from ZODB import POSException

class NEOStorageError(POSException.StorageError):
    pass

class NEOStorageNotFoundError(NEOStorageError):
    pass

class NEOStorageDoesNotExistError(NEOStorageNotFoundError):
    """
    This error is a refinement of NEOStorageNotFoundError: this means
    that some object was not found, but also that it does not exist at all.
    """
    pass

class NEOStorageCreationUndoneError(NEOStorageDoesNotExistError):
    """
    This error is a refinement of NEOStorageDoesNotExistError: this means that
    some object existed at some point, but its creation was undone.
    """

