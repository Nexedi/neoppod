#
# Copyright (C) 2015  Nexedi SA
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

from . import logging
from .event import EventManager
from .node import NodeManager


class BaseApplication(object):

    ssl = None

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
