#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2018  Nexedi SA and Contributors.
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
"""tcpenalty - detect C-state related time penalties"""

# XXX not finished.
# intel_idle/menu:
#   69d25870 (cpuidle: fix the menu governor to boost IO performance)
#   1f85f87d (cpuidle: add a repeating pattern detector to the menu governor)
#
#   69a37bea (cpuidle: Quickly notice prediction failure for repeat mode)
#   e11538d1 (cpuidle: Quickly notice prediction failure in general case)
#   `- both reverted in __14851912__ (!) and 228b3023.
#
#   c96ca4fb (cpuidle: Get typical recent sleep interval)  <- filters out outliers NOTE (!  ?)
#
#   efddfd90 (cpuidle,menu: smooth out measured_us calculation)
#   e132b9b3 (cpuidle: menu: use high confidence factors only when considering polling)
#   `- (POLL vs C1 fine tune)
#   `-> reverted in 0c313cb2 (cpuidle: menu: Fall back to polling if next timer event is near)
#   ^^^ (not important)



from __future__ import print_function

import os, struct, threading
import psutil
from collections import namedtuple
from glob import glob
from os.path import basename
from time import time

from tcpu import B, readfile


# PipeChan is `chan float` which uses OS pipe for communication.
class PipeChan(object):
    def __init__(self):
        self._r, self._w = os.pipe()

    # send sends a float t.
    def send(self, t):
        data = struct.pack('>d', t)
        while len(data) > 0:
            n = os.write(self._w, data)
            data = data[n:]

    # recv receives a float sent.
    #
    # returns either t (float) or None if the channel was closed.
    def recv(self):
        data = ''
        while len(data) != 8:
            b = os.read(self._r, 8 - len(data))
            if not b:
                if len(data) > 0:
                    raise RuntimeError('pipechan: unexpected EOF at half-way receive')
                return None # closed

            data += b

        (t,) = struct.unpack('>d', data)
        return t


    # close closes the sending end of the channel, this way signalling to
    # receiver there will be no more data.
    def close(self):
        os.close(self._w)


syscpu  = '/sys/devices/system/cpu'
cpuv    = []    # ex 0, 1, 2, ...
cstatev = []    # of (name, latency, dir-under-cpuidle/ (ex state0))

CState = namedtuple('CState', 'name, latency, dir')

# initcpu initializes cpuv & cstatev
def init_cpu():
    global cpuv, cstatev
    cpuv_ = glob('%s/cpu[0-9]*' % syscpu)
    cpuv = [int(basename(_)[3:]) for _ in cpuv_]
    cpuv.sort()

    for cpu in cpuv:
        sys_cpuidle = '%s/cpu%d/cpuidle' % (syscpu, cpu)
        cstatev_ = glob('%s/state[0-9]*' % sys_cpuidle)
        cstatev_ = [int(basename(_)[5:]) for _ in cstatev_] # numeric
        cstatev_.sort()
        cv = [] # of CState
        for s in cstatev_:
            sdir  = 'state%d' % s
            spath = '%s/%s' % (sys_cpuidle, sdir)
            slat  = readfile('%s/latency' % spath)
            sname = readfile('%s/name' % spath)

            slat = int(slat)
            sname= sname.rstrip()   # \n
            cv.append(CState(sname, slat, sdir))

        if not cstatev:
            cstatev = cv

        if cv != cstatev:
            raise RuntimeError('cpu%d: cstates are different to earlier cpus: %s vs %s' % (cpu, cv, cstatev))


CStateStat = namedtuple('CStateStat', 'nusage, time')

# idle_stat returns current idle usage statistic for cpu.
#
# returns -> [] of #usage.
#
# each element of returned [] corresponds to according c-state in cstatev.
def idle_stat(cpu):
    sys_cpuidle = '%s/cpu%d/cpuidle' % (syscpu, cpu)
    statv = []
    for cs in cstatev:
        nusage = readfile('%s/%s/usage' % (sys_cpuidle, cs.dir))
        time   = readfile('%s/%s/time'  % (sys_cpuidle, cs.dir))
        nusage = int(nusage)
        time   = float(time) * 1E-6 # in µs
        statv.append(CStateStat(nusage, time))

    return statv



us = 1E-6   # µs
from random import random


def main():
    # we need support from xos.pyx, but do not force cython dependency on the rest
    import pyximport; pyximport.install()
    import xos

    init_cpu()

    # 2 OS threads are exchanging messages with each other.
    # - the first thread simulates a client - it sends requests to the second
    #   thread to do some work of time t.
    # - the second thread simulates a server - it receives requests from client
    #   and busyloops for the requested time t.

    reqc  = PipeChan()      # client -> worker "do work t"
    respc = PipeChan()      # client <- worker "done work t'"
                            # (t' is the actual time worker spent doing the work)

    # measure overhead of send/recv via os pipe but without context switch
    def bench_rtt_noctxwitch(b, _):
        n = b.N
        i = 0
        while i < n:
            reqc.send(i)        # non-blocking if < 1 page
            i_ = reqc.recv()
            assert i == i_
            i += 1

    #for i in range(4):
    #    benchit(bench_rtt_noctxwitch, 0)

    # XXX need to sleep after it    XXX really?

    test_cpuv = [0, 1]   # cpu interesting for us             # XXX also check with cpu2 which is another hyperthread with cpu0

    def worker():
        try:
            _worker()
        finally:
            respc.close()

    def _worker():
        # pin worker thread to second cpu cpu1
        myproc = psutil.Process(xos.gettid())
        myproc.cpu_affinity([test_cpuv[1]])

        #print('worker:\t@cpu%d' % myproc.cpu_num())
        while 1:
            t = reqc.recv()
            #print('worker: rx %r' % t)

            # signal to quit
            if t is None:
                #print('worker:\t@cpu%d' % myproc.cpu_num())
                return

            # busyloop for t
            tstart = time()
            while 1:
                now = time()
                t_ = now - tstart
                if t_ >= t:
                    break

                i = 0
                while i < 100:
                    i += 1

            # send back the time we have been busy-looping
            #print('worker: tx %r' % t_)
            respc.send(t_)


    twork = threading.Thread(target=worker)
    twork.start()

    # measure communication overhead
    def bench_rtt(b, t, thistv_by10us):
        n = b.N
        i = 0
        while i < n:
            #print('client: tx %r' % t)
            tstart = time()
            reqc.send(t + random()*80*us)  # XXX random
            t_ = respc.recv()
            if t_ is None:
                raise RuntimeError('worker exited prematurely')
            tend = time()
            dt_us = (tend - tstart) / us
            thist_slot = int(dt_us // 10)
            if thist_slot < len(thistv_by10us) - 1:
                thistv_by10us[thist_slot] += 1
            else:
                thistv_by10us[-1] +=1

            #print('client: rx %r' % t_)
            i += 1

    def tmain():
        # pin main thread to cpu0
        myproc = psutil.Process(xos.gettid())
        myproc.cpu_affinity([test_cpuv[0]])


        #print('main:\t@cpu%d' % myproc.cpu_num())
        for i in range(10):
            idle_start = {}
            for cpu in test_cpuv:
                idle_start[cpu] = idle_stat(cpu)

            print()

            thistv_by10us = [0]*100

            t = 150*us
            b = B()
            b.N = 10000 * 1.0
            b.reset_timer()
            b.start_timer()
            bench_rtt(b, t, thistv_by10us)
            b.stop_timer()

            print('Benchmarkrtt/work=%.0fµs\t%d\t%.1f µs/op' % (
                    t * 1E6, b.N, b.total_time() / b.N * 1E6))

            idle_stop = {}
            for cpu in test_cpuv:
                idle_stop[cpu] = idle_stat(cpu)

            for cpu in test_cpuv:
                s = 'cpu%d:' % cpu
                for i, (start, stop) in  enumerate(zip(idle_start[cpu], idle_stop[cpu])):
                    dnusage_op = float(stop.nusage - start.nusage) / b.N
                    dtime_op   = float(stop.time  - start.time)    / b.N

                    if dtime_op > 0.1*us:
                        s += ' %s·%.3f/op(%.1fµs/op)' % (
                                cstatev[i].name, dnusage_op, dtime_op / us)
                    else:
                        s += ' %s      ' % (' '*len(cstatev[i].name))
                print(s)


            ntotal = sum(thistv_by10us)
            for i, n in enumerate(thistv_by10us):
                if n == 0:
                    continue
                if i < len(thistv_by10us) - 1:
                    tslot = '%3d µs' % ((i+1)*10,)
                else:
                    tslot = '     ∞'

                s = '< %s\t%d\t| %s' % (tslot, n, '*'*(50*n//ntotal))
                print(s)

        #print('main:\t@cpu%d' % myproc.cpu_num())

    try:
        tmain()
    finally:
        reqc.close()
        twork.join()


if __name__ == '__main__':
    main()
