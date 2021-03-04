##############################################################################
#
# Copyright (C) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

def patch():
    # For msgpack & Py2/ZODB5.
    try:
        from zodbpickle import binary
        binary._pack = bytes.__str__
    except ImportError:
        pass

    from hashlib import md5
    from ZODB.Connection import Connection

    H = lambda f: md5(f.func_code.co_code).hexdigest()

    if hasattr(Connection, '_handle_serial'): # merged upstream ?
        assert hasattr(Connection, '_warn_about_returned_serial')

    # sync() is used to provide a "network barrier", which is required for
    # NEO & ZEO to make sure our view of the storage includes all changes done
    # so far by other clients. But a round-trip to the server introduces
    # latency so it must not be done when it's not useful. Note also that a
    # successful commit (which ends with a response from the master) already
    # acts as a "network barrier".
    # BBB: What this monkey-patch does has been merged in ZODB5.
    if not hasattr(Connection, '_flush_invalidations'):
        return

    assert H(Connection.afterCompletion) in (
        'cd3a080b80fd957190ff3bb867149448', # Python 2.7
        )

    def afterCompletion(self, *ignored):
        self._readCurrent.clear()
        # PATCH: do not call sync()
        self._flush_invalidations()
    Connection.afterCompletion = afterCompletion

patch()

from . import app # set up signal handlers early enough to do it in the main thread
