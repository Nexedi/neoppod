#
# Copyright (C) 2018-2019  Nexedi SA
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

from contextlib import contextmanager
from neo import *
from ZConfig import ConfigurationError
from ZODB.config import databaseFromString
from .. import Patch
from . import ClientApplication, NEOThreadedTest, with_cluster
from neo.client import Storage

def databaseFromDict(**kw):
    return databaseFromString("%%import neo.client\n"
        "<zodb>\n <NEOStorage>\n%s </NEOStorage>\n</zodb>\n"
        % ''.join('  %s %s\n' % x for x in six.iteritems(kw)))

class ConfigTests(NEOThreadedTest):

    dummy_required = {'name': 'cluster', 'master_nodes': '127.0.0.1:10000'}

    @contextmanager
    def _db(self, cluster, **kw):
        kw['name'] = cluster.name
        kw['master_nodes'] = cluster.master_nodes
        def newClient(_, *args, **kw):
            client = ClientApplication(*args, **kw)
            t.append(client.poll_thread)
            return client
        t = []
        with Patch(Storage, Application=newClient):
            db = databaseFromDict(**kw)
        try:
            yield db
        finally:
            db.close()
            cluster.join(t)

    @with_cluster()
    def testCompress(self, cluster):
        kw = self.dummy_required.copy()
        valid = ['false', 'true', 'zlib', 'zlib=9']
        for kw['compress'] in '9', 'best', 'zlib=0', 'zlib=100':
            # BBB: DataConversionError since ZConfig 3.4.0
            self.assertRaises(ConfigurationError, databaseFromDict, **kw)
        for compress in valid:
            with self._db(cluster, compress=compress) as db:
                self.assertEqual((0,0,b''), db.storage.app.compress(b''))
