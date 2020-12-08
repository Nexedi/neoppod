#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2020  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Free Software licenses or any of the Open Source
# Initiative approved licenses and Convey the resulting work. Corresponding
# source of such a combination shall include the source code for all other
# software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# See https://www.nexedi.com/licensing for rationale and options.
"""runneo.py runs NEO/py cluster for NEO/go testing.

Usage: runneo.py <workdir> <cluster-name>   [k1=v1] [k2=v2] ...

<workdir>/ready is created with address of master after spawned cluster becomes
operational.
"""

from neo.tests.functional import NEOCluster
from golang import func, defer

import sys, os
from time import sleep
from signal import signal, SIGTERM


@func
def main():
    workdir     = sys.argv[1]
    clusterName = sys.argv[2]
    readyf      = workdir + "/ready"

    kw = {}
    for arg in sys.argv[3:]:
        k, v = arg.split('=')
        kw[k] = v


    flags = ''
    def sinfo(msg): return "I: runneo.py: %s/%s%s: %s" % (workdir, clusterName, flags, msg)
    def info(msg):  print(sinfo(msg))

    # SIGTERM -> exit gracefully, so that defers are run
    def _(sig, frame):
        raise SystemExit(sinfo("terminated"))
    signal(SIGTERM, _)

    flags = ' !ssl'
    ca   = kw.pop('ca',   None)
    cert = kw.pop('cert', None)
    key  = kw.pop('key',  None)
    if ca or cert or key:
        if not (ca and cert and key):
            raise RuntimeError(sinfo("incomplete ca/cert/key provided"))
        # neo/py does `NEOCluster.SSL = neo.test.SSL` (= (ca.crt, node.crt, node.key) )
        flags = ' ssl'
        NEOCluster.SSL = (ca, cert, key)

    if kw:
        raise RuntimeError(sinfo("unexpected flags: %s" % kw))

    cluster = NEOCluster([clusterName], adapter='SQLite', name=clusterName, temp_dir=workdir)
    cluster.start()
    defer(cluster.stop)

    cluster.expectClusterRunning()
    info("started master(s): %s" % (cluster.master_nodes,))

    # dump information about ready cluster into readyfile
    with open("%s.tmp" % readyf, "w") as f:
        f.write(cluster.master_nodes)    # XXX ' ' separated if multiple masters
    os.rename("%s.tmp" % readyf, readyf) # atomic

    def _():
        os.unlink(readyf)
    defer(_)

    while 1:
        sleep(1)


if __name__ == '__main__':
    main()
