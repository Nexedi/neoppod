#
# Copyright (C) 2015  Nexedi SA
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

from .event import EventManager
from .node import NodeManager


class BaseApplication(object):

    def __init__(self, dynamic_master_list=None):
        self._handlers = {}
        self.em = EventManager()
        self.nm = NodeManager(dynamic_master_list)

    # XXX: Do not implement __del__ unless all references to the Application
    #      become weak.
    #      Due to cyclic references, Python < 3.4 would never call it unless
    #      it's closed explicitly, and in this case, there's nothing to do.

    def close(self):
        self.nm.close()
        self.em.close()
        self.__dict__.clear()
