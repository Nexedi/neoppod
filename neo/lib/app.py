#
# Copyright (C) 2015-2019  Nexedi SA
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

from . import logging, util
from .config import OptionList
from .event import EventManager
from .node import NodeManager


def buildOptionParser(cls):
    parser = cls.option_parser = cls.OptionList()
    _ = parser.path
    _('l', 'logfile',
        help="log debugging information to specified SQLite DB")
    _('ca',  help="(SSL) certificate authority in PEM format")
    _('cert', help="(SSL) certificate in PEM format")
    _('key', help="(SSL) private key in PEM format")
    cls._buildOptionParser()
    return cls


class BaseApplication(object):

    class OptionList(OptionList):

        def parse(self, argv=None):
            config = OptionList.parse(self, argv)
            ssl = (
                config.pop('ca', None),
                config.pop('cert', None),
                config.pop('key', None),
            )
            if any(ssl):
                config['ssl'] = ssl
            return config

    server = None
    ssl = None

    @classmethod
    def addCommonServerOptions(cls, section, bind, masters='127.0.0.1:10000'):
        _ = cls.option_parser.group('server node')
        _.path('f', 'file', help='specify a configuration file')
        _('s', 'section', default=section,
            help='specify a configuration section')
        _('c', 'cluster', required=True, help='the cluster name')
        _('m', 'masters', parse=util.parseMasterList,
            help='space-separated list of master node addresses',
            **{'default': masters} if masters else {'type': lambda x: x or ''})
        _('b', 'bind', default=bind,
            parse=lambda x: util.parseNodeAddress(x, 0),
            help='the local address to bind to')
        _.path('D', 'dynamic-master-list',
            help='path of the file containing dynamic master node list')

    def __init__(self, ssl=None, dynamic_master_list=None):
        if ssl:
            if not all(ssl):
                raise ValueError("To enable encryption, 3 files must be"
                    " provided: the CA certificate, and the certificate"
                    " of this node with its private key.")
            ca, cert, key = ssl
            import ssl
            version, version_name = max((getattr(ssl, k), k)
                for k in dir(ssl) if k.startswith("PROTOCOL_TLSv"))
            self.ssl = context = ssl.SSLContext(version)
            context.options |= (0
                | ssl.OP_CIPHER_SERVER_PREFERENCE
                | ssl.OP_NO_COMPRESSION
                )
            context.set_ciphers(ssl._RESTRICTED_SERVER_CIPHERS)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(ca)
            context.load_cert_chain(cert, key)
            context.verify_flags |= ssl.VERIFY_X509_STRICT | (
                context.cert_store_stats()['crl'] and ssl.VERIFY_CRL_CHECK_LEAF)
            logging.info("TLS %s enabled for %s",
                float(version_name[13:].replace("_", ".")), self)
        self._handlers = {}
        self.em = EventManager()
        self.nm = NodeManager(dynamic_master_list)

    # XXX: Do not implement __del__ unless all references to the Application
    #      become weak.
    #      Due to cyclic references, Python < 3.4 would never call it unless
    #      it's closed explicitly, and in this case, there's nothing to do.

    def close(self):
        self.nm.close()
        self.em.close()
        self.__dict__.clear()

    def log(self):
        self.em.log()
        self.nm.log()
        pt = self.__dict__.get('pt')
        if pt is not None:
            pt.log()

    def setUUID(self, uuid):
        if self.uuid != uuid:
            self.uuid = uuid
            logging.node(self.name, uuid)
