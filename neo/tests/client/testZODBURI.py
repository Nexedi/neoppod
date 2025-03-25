#
# Copyright (C) 2017-2019  Nexedi SA
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

import os
import unittest
from neo.client.zodburi import _resolve_uri


testv = [
    # [] of (uri, env, zconf_ok, dbkw_ok)
    ("neo://dbname@master", {},
     """\
  master_nodes\tmaster
  name\tdbname
""",
     {}),

    ("neo://db2@master1:port1,master2:port2,master3:port3", {},
     """\
  master_nodes\tmaster1:port1 master2:port2 master3:port3
  name\tdb2
""",
     {}),

    ("neo://db3@master1,master2:port2?read_only=true", {},
     """\
  master_nodes\tmaster1 master2:port2
  name\tdb3
  read-only\ttrue
""",
     {}),

    ("neo://db4@[2001:67c:1254:2a::1]:1234,master2:port2?read_only=false"
     "&compress=true&logfile=xxx&alpha=111&dynamic_master_list=zzz&ca=qqq"
     "&cert=rrr&key=sss&beta=222", {},
     """\
  master_nodes\t[2001:67c:1254:2a::1]:1234 master2:port2
  name\tdb4
  read-only\tfalse
  compress\ttrue
  logfile\txxx
  dynamic_master_list\tzzz
  ca\tqqq
  cert\trrr
  key\tsss
""",
     {"alpha": "111", "beta": "222"}),

    ("neos://db5@master?ca=~/path/to/ca&cert=/path/to/cert&key=key.crt&logfile=xxx", {},
     """\
  master_nodes\tmaster
  name\tdb5
  ca\t~/path/to/ca
  cert\t/path/to/cert
  key\tkey.crt
  logfile\txxx
""",
     {}),

    ("neos://db6@master?ca=~/path/to/ca&cert=/path/to/cert&key=key.crt&logfile=xxx",
     {"NEO_CA": "/ca.crt", "NEO_CERT": "/cert.crt", "NEO_KEY": "/key.crt"},
     """\
  master_nodes\tmaster
  name\tdb6
  ca\t~/path/to/ca
  cert\t/path/to/cert
  key\tkey.crt
  logfile\txxx
""",
     {}),

    ("neos://db7@master?ca=~/path/to/ca&key=key.crt&logfile=xxx",
     {"NEO_CA": "/ca.crt", "NEO_CERT": "/cert.crt", "NEO_KEY": "/key.crt"},
     """\
  master_nodes\tmaster
  name\tdb7
  ca\t~/path/to/ca
  key\tkey.crt
  logfile\txxx
  cert\t/cert.crt
""",
     {}),

    ("neos://db8@master?logfile=xxx",
     {"NEO_CA": "/ca.crt", "NEO_CERT": "/cert.crt", "NEO_KEY": "/key.crt"},
     """\
  master_nodes\tmaster
  name\tdb8
  logfile\txxx
  ca\t/ca.crt
  cert\t/cert.crt
  key\t/key.crt
""",
     {}),

    ("neos://db9@master",
     {"NEO_CA": "/ca.crt", "NEO_CERT": "/cert.crt", "NEO_KEY": "/key.crt"},
     """\
  master_nodes\tmaster
  name\tdb9
  ca\t/ca.crt
  cert\t/cert.crt
  key\t/key.crt
""",
     {}),
]



class ZODBURITests(unittest.TestCase):

    def test_zodburi(self):
        # invalid schema / path / fragment
        self.assertRaises(ValueError, _resolve_uri, "http://db@master")
        self.assertRaises(ValueError, _resolve_uri, "neo://db@master/path")
        self.assertRaises(ValueError, _resolve_uri, "neo://db@master#frag")

        # db @ master not fully specified
        self.assertRaises(ValueError, _resolve_uri, "neo://master")

        # master_nodes and name provided in options (they come in netloc)
        self.assertRaises(ValueError, _resolve_uri, "neo://db@master?master_nodes=a,b,c")
        self.assertRaises(ValueError, _resolve_uri, "neo://db@master?name=zzz")

        # verify zodburi resolver produces expected zconfig
        for uri, env, zconf_ok, dbkw_ok in testv:
            zconf_ok = "%import neo.client\n<NEOStorage>\n" + zconf_ok + \
                       "</NEOStorage>\n"

            envsave = os.environ
            os.environ = env
            try:
                zconf, dbkw = _resolve_uri(uri)
            finally:
                os.environ = envsave
            self.assertMultiLineEqual(zconf, zconf_ok)
            self.assertEqual(dbkw, dbkw_ok)
