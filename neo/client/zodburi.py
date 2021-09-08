#
# Copyright (C) 2017-2021  Nexedi SA
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

    neo(s)://[credentials@]master1,master2,...,masterN/name?options
"""

import ZODB.config
import ZConfig

from cStringIO import StringIO
from collections import OrderedDict
from urlparse import urlsplit, parse_qsl

# _credopts defines which options correspond to credentials
_credopts = {'ca', 'cert', 'key'}

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
    for opt in _credopts:
        assert opt in options, opt

    return options

# canonical_opt_name returns "oPtion_nAme" as "option-name"
def canonical_opt_name(name):
    return name.lower().replace('_', '-')

# worker entrypoint for resolve_uri and tests
def _resolve_uri(uri):
    scheme, netloc, path, query, frag = urlsplit(uri)

    if scheme not in ("neo", "neos"):
        raise ValueError("invalid uri: %s : expected neo:// or neos:// scheme" % uri)
    if frag != "":
        raise ValueError("invalid uri: %s : non-empty fragment" % uri)

    # name is given as path
    if path.startswith("/"):
        path = path[1:]
    name = path
    if name == '':
        raise ValueError("invalid uri: %s : cluster name not specified" % uri)

    # extract master list and credentials from netloc
    cred, masterloc = '', netloc
    if '@' in netloc:
        cred, masterloc = netloc.split('@', 1)
    master_list = masterloc.split(',')

    neokw = OrderedDict()
    neokw['master_nodes'] = ' '.join(master_list)
    neokw['name'] = name

    # parse credentials
    if cred:
        if scheme != "neos":
            raise ValueError("invalid uri: %s : credentials can be specified only with neos:// scheme" % uri)
        # ca=ca.crt;cert=my.crt;key=my.key
        cred = cred.replace(';', '&') # ; is no longer in default separators set bugs.python.org/issue42967
        for k, v in OrderedDict(parse_qsl(cred)).items():
            if k not in _credopts:
                raise ValueError("invalid uri: %s : unexpected credential %s" % (uri, k))
            neokw[k] = v

    # get options from query: only those that are defined by NEO schema go to
    # storage - rest are returned as database options
    dbkw = {}
    neo_options = neo_zconf_options()
    for k, v in OrderedDict(parse_qsl(query)).items():
        if k in _credopts:
            raise ValueError("invalid uri: %s : option %s must be in credentials" % (uri, k))
        elif k in neo_options:
            neokw[k] = v
        else:
            # it might be option for storage, but not in canonical form e.g.
            # read_only -> read-only  (zodburi world settled on using "_" and
            # ZConfig world on "-" as separators)
            k2 = canonical_opt_name(k)
            if k2 in neo_options:
                neokw[k2] = v

            # else keep this kv as db option
            else:
                dbkw[k] = v


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
