#
# Copyright (C) 2006-2009  Nexedi SA

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

import unittest

from neo.tests import NeoTestBase
from neo import util

class UtilTests(NeoTestBase):

    def test_getNextTID(self):
        tid0 = '\0' * 8
        tid1 = util.getNextTID(tid0)
        self.assertTrue(tid1 > tid0)
        tid2 = util.getNextTID(tid1)
        self.assertTrue(tid2 > tid1)
        tidx = '\0' + chr(0xFF) * 7
        tid3 = util.getNextTID(tidx)
        self.assertTrue(tid3 > tidx)


if __name__ == "__main__":
    unittest.main()

