#
# Copyright (C) 2006-2009  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

class EnumItem(int):
    """
      Enumerated value type.
      Not to be used outside of Enum class.
    """
    def __new__(cls, enum, name, value):
        instance = super(EnumItem, cls).__new__(cls, value)
        instance.enum = enum
        instance.name = name
        return instance

    def __eq__(self, other):
        """
          Raise if compared type doesn't match.
        """
        if not isinstance(other, EnumItem):
            raise TypeError, 'Comparing an enum with an int.'
        if other.enum is not self.enum:
            raise TypeError, 'Comparing enums of incompatible types: %s ' \
                             'and %s' % (self, other)
        return int(other) == int(self)

    def __ne__(self, other):
        return not(self == other)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<EnumItem %r (%r) of %r>' % (self.name, int(self), self.enum)

class Enum(object):
    """
      C-style enumerated type support with extended typechecking.
      Instantiate with a dict whose keys are variable names and values are
      the value of that variable.
      Variables are added to module's globals and can be used directly.

      The purpose of this class is purely to prevent developper from
      mistakenly comparing an enumerated value with a value from another enum,
      or even not from any enum at all.
    """
    def __init__(self, value_dict):
        global_dict = globals()
        self.enum_dict = enum_dict = {}
        self.str_enum_dict = str_enum_dict = {}
        for key, value in value_dict.iteritems():
            # Only integer types are supported. This should be enough, and
            # extending support to other types would only make moving to other
            # languages harder.
            if not isinstance(value, int):
                raise TypeError, 'Enum class only support integer values.'
            item = EnumItem(self, key, value)
            global_dict[key] = enum_dict[value] = item
            str_enum_dict[key] = item

    def get(self, value, default=None):
        return self.enum_dict.get(value, default)

    def getFromStr(self, value, default=None):
        return self.str_enum_dict.get(value, default)

    def __getitem__(self, value):
        return self.enum_dict[value]

