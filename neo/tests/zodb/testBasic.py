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

import time
import unittest
from ZODB.tests.BasicStorage import BasicStorage
from ZODB.tests.StorageTestBase import StorageTestBase

from . import ZODBTestCase
from .. import Patch, threaded

class TIC_LOOP(object):

    def __init__(self):
        self.stop = time.time() + 10

    def __iter__(self):
        def tic_loop(t=time.time, x=self.stop):
            while t() < x:
                yield
        return tic_loop()

class BasicTests(ZODBTestCase, StorageTestBase, BasicStorage):

    def check_checkCurrentSerialInTransaction(self):
        with Patch(threaded, TIC_LOOP=TIC_LOOP()):
            super(BasicTests, self).check_checkCurrentSerialInTransaction()

    # The test expects that both load & lastTransaction would be blocked
    # as long as the tpc_finish callback has not finished, taking more
    # than .1 second. ZODB 5.6.0 clarified that lastTransaction() can
    # return immediately with the previous last TID rather than blocking
    # until it is allowed to return the new last TID.
    check_tid_ordering_w_commit = unittest.skip("ZODB PR #316")(
        BasicStorage.check_tid_ordering_w_commit)
