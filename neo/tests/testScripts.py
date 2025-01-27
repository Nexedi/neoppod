#
# Copyright (C) 2025 Nexedi SA
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

import sys, unittest
from cStringIO import StringIO
from . import NeoUnitTestBase, Patch

class ScriptTests(NeoUnitTestBase):

    def testHelp(self):
        from neo.scripts import \
            neoctl, neolog, neomaster, neomigrate, reflink, simple
        def check(script, args):
            stdout = StringIO()
            stderr = StringIO()
            with Patch(sys, stdout=stdout), Patch(sys, stderr=stderr):
                script.main(args)
            self.assertTrue(stdout.getvalue())
            self.assertFalse(stderr.getvalue())
        check(neoctl, [])
        for script in neoctl, neolog, neomaster, neomigrate, reflink, simple:
            with self.assertRaises(SystemExit) as cm:
                check(script, ['--help'])
            self.assertFalse(cm.exception.code)

if __name__ == '__main__':
    unittest.main()
