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
"""NEO URI resolver for zodburi

URI format:

    neo://name@master1,master2,...,masterN?options
    neos://----//---- with $NEO_CA, $NEO_CERT and $NEO_KEY providing defaults for ca/cert/key options
"""

import ZODB.config
import ZConfig

import os
from cStringIO import StringIO
from collections import OrderedDict
from urlparse import urlsplit, parse_qsl

# neo_zconf_options returns set of zconfig options supported by NEO storage
def neo_zconf_options():
    neo_schema = """<schema>
      <import package="ZODB" />
      <import package="neo.client" />
    </schema>"""

    neo_schema = StringIO(neo_schema)
    neo_schema = ZConfig.loadSchemaFile(neo_schema)
    neo_storage_zconf = neo_schema.gettype('NeoStorage')

    options = {k for k, _ in neo_storage_zconf}
    assert 'master_nodes' in options
    assert 'name' in options

    return options

# worker entrypoint for resolve_uri and tests
def _resolve_uri(uri):
    scheme, netloc, path, query, frag = urlsplit(uri)

    if scheme not in ("neo", "neos"):
        raise ValueError("invalid uri: %s : expected neo:// or neos:// scheme" % uri)
    if path != "":
        raise ValueError("invalid uri: %s : non-empty path" % uri)
    if frag != "":
        raise ValueError("invalid uri: %s : non-empty fragment" % uri)

    # extract master list and name from netloc
    name, masterloc = netloc.split('@', 1)
    master_list = masterloc.split(',')

    neokw = OrderedDict()
    neokw['master_nodes'] = ' '.join(master_list)
    neokw['name'] = name
    def setopt(k, v):
        if k in ('master_nodes', 'name'):
            raise ValueError("invalid uri: %s : invalid option %s" % (uri, k))
        neokw[k] = v

    # get options from query: only those that are defined by NEO schema go to
    # storage - rest are returned as database options
    dbkw = {}
    neo_options = neo_zconf_options()
    for k, v in OrderedDict(parse_qsl(query)).items():
        if k in neo_options:
            setopt(k, v)
        else:
            # it might be option for storage, but not in canonical form e.g.
            # read_only -> read-only  (zodburi world settled on using "_" and
            # ZConfig world on "-" as separators)
            k2 = k.replace('_', '-')
            if k2 in neo_options:
                setopt(k2, v)

            # else keep this kv as db option
            else:
                dbkw[k] = v

    # neo://   use TLS only if ca/cert/key are explicitly specified in uri
    # neos://  force TLS to be used and take ca/cert/key from environment if
    #          TLS credentials are not explicitly specified in uri
    if scheme == "neos":
        for k in ('ca', 'cert', 'key'):
            if k in neokw:
                continue
            kenv = "NEO_" + k.upper()
            v = os.environ.get(kenv)
            if not v:
                raise ValueError("invalid uri: %s : option %s not specified "
                                 "and $%s is also not set" % (uri, k, kenv))
            setopt(k, v)

    # now we have everything. Let ZConfig do actual work for validation options
    # and borning the storage
    neozconf = """%import neo.client
<NEOStorage>
"""
    for k, v in neokw.items():
        neozconf += "  %s\t%s\n" % (k, v)

    neozconf += "</NEOStorage>\n"

    return neozconf, dbkw

# resolve_uri resolves uri according to neo:// schema.
# see module docstring for uri format.
def resolve_uri(uri):
    neozconf, dbkw = _resolve_uri(uri)

    def factory():
        return ZODB.config.storageFromString(neozconf)

    return factory, dbkw
