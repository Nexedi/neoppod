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

from neo.locking import LockUser

"""
  Usage example:

    from neo import attributeTracker

    class Foo(object):

        ...

        def assertBar(self, expected_value):
            if self.bar_attr != expected_value:
                attributeTracker.whoSet(self, 'bar_attr')

    attributeTracker.track(Foo)
"""

MODIFICATION_CONTAINER_ID = '_attribute_tracker_dict'

def tracker_setattr(self, attr, value, setattr):
    modification_container = getattr(self, MODIFICATION_CONTAINER_ID, None)
    if modification_container is None:
        modification_container = {}
        setattr(self, MODIFICATION_CONTAINER_ID, modification_container)
    modification_container[attr] = LockUser()
    setattr(self, attr, value)

def track(klass):
    original_setattr = klass.__setattr__
    def klass_tracker_setattr(self, attr, value):
        tracker_setattr(self, attr, value, original_setattr)
    klass.__setattr__ = klass_tracker_setattr

def whoSet(instance, attr):
    result = getattr(instance, MODIFICATION_CONTAINER_ID, None)
    if result is not None:
        result = result.get(attr)
    if result is not None:
        result = result.formatStack()
    return result

