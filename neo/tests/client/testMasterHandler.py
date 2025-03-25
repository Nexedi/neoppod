#
# Copyright (C) 2009-2019  Nexedi SA
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

from .. import Mock, NeoUnitTestBase
from neo.client.handlers.master import PrimaryAnswersHandler
from neo.client.exception import NEOStorageError

class MasterHandlerTests(NeoUnitTestBase):

    def setUp(self):
        super(MasterHandlerTests, self).setUp()
        self.handler = PrimaryAnswersHandler(self.getFakeApplication())

    def test_answerPack(self):
        self.assertRaises(NEOStorageError, self.handler.answerPack, None, False)
        # Check it doesn't raise
        self.handler.answerPack(None, True)
