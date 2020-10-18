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

Usage: runneopy <workdir> <readyfile>   XXX + (**kw for NEOCluster)
XXX
"""

from neo.tests.functional import NEOCluster
from golang import func, defer


@func
def main():
    workdir = sys.argv[1]
    readyf  = sys.argv[1]

    cluster = NEOCluster(['1'], adapter='SQLite', temp_dir=workdir)   # XXX +kw
    cluster.start()
    defer(cluster.stop)

    cluster.expectClusterRunning()
    zstor = cluster.getZODBStorage()

    # dump information about ready cluster into readyfile
    with open("%s.tmp" % readyf, "w") as f:
        # XXX master addresses
        # XXX + addresses of other nodes?
        f.write("...")
    os.rename("%s.tmp" % readyf, readyf) # atomic

    def _():
        os.unlink(readyf)
    defer(_)


    # XXX loop forever



if __name__ == '__main__':
    main()
