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

from ZODB.config import BaseConfig

class NeoStorage(BaseConfig):

    def open(self):
        from .Storage import Storage
        config = self.config
        return Storage(**{k: getattr(config, k)
                          for k in config.getSectionAttributes()})

def compress(value):
    from ZConfig.datatypes import asBoolean
    try:
        return asBoolean(value)
    except ValueError:
        from neo.lib.compress import parseOption
    return parseOption(value)
