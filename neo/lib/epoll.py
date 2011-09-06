#
# Copyright (C) 2006-2010  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

r"""This is an epoll(4) interface available in Linux 2.6. This requires
ctypes <http://python.net/crew/theller/ctypes/>."""

from ctypes import cdll, Union, Structure, \
        c_void_p, c_int, byref
try:
    from ctypes import c_uint32, c_uint64
except ImportError:
    from ctypes import c_uint, c_ulonglong
    c_uint32 = c_uint
    c_uint64 = c_ulonglong
from os import close
from errno import EINTR, EAGAIN

libc = cdll.LoadLibrary('libc.so.6')
epoll_create = libc.epoll_create
epoll_wait = libc.epoll_wait
epoll_ctl = libc.epoll_ctl
errno = c_int.in_dll(libc, 'errno')

EPOLLIN = 0x001
EPOLLPRI = 0x002
EPOLLOUT = 0x004
EPOLLRDNORM = 0x040
EPOLLRDBAND = 0x080
EPOLLWRNORM = 0x100
EPOLLWRBAND = 0x200
EPOLLMSG = 0x400
EPOLLERR = 0x008
EPOLLHUP = 0x010
EPOLLONESHOT = (1 << 30)
EPOLLET = (1 << 31)

EPOLL_CTL_ADD = 1
EPOLL_CTL_DEL = 2
EPOLL_CTL_MOD = 3

class EpollData(Union):
    _fields_ = [("ptr", c_void_p),
                ("fd", c_int),
                ("u32", c_uint32),
                ("u64", c_uint64)]

class EpollEvent(Structure):
    _fields_ = [("events", c_uint32),
                ("data", EpollData)]
    _pack_ = 1

class Epoll(object):
    efd = -1

    def __init__(self):
        self.efd = epoll_create(10)
        if self.efd == -1:
            raise OSError(errno.value, 'epoll_create failed')

        self.maxevents = 1024 # XXX arbitrary
        epoll_event_array = EpollEvent * self.maxevents
        self.events = epoll_event_array()

    def poll(self, timeout=1):
        if timeout is None:
            timeout = -1
        else:
            timeout *= 1000
            timeout = int(timeout)
        while True:
            n = epoll_wait(self.efd, byref(self.events), self.maxevents,
                           timeout)
            if n == -1:
                e = errno.value
                # XXX: Why 0 ? Maybe due to partial workaround in neo.lib.debug.
                if e in (0, EINTR, EAGAIN):
                    continue
                else:
                    raise OSError(e, 'epoll_wait failed')
            else:
                readable_fd_list = []
                writable_fd_list = []
                error_fd_list = []
                for i in xrange(n):
                    ev = self.events[i]
                    fd = int(ev.data.fd)
                    if ev.events & EPOLLIN:
                        readable_fd_list.append(fd)
                    if ev.events & EPOLLOUT:
                        writable_fd_list.append(fd)
                    if ev.events & (EPOLLERR | EPOLLHUP):
                        error_fd_list.append(fd)
                return readable_fd_list, writable_fd_list, error_fd_list

    def register(self, fd):
        ev = EpollEvent()
        ev.data.fd = fd
        ret = epoll_ctl(self.efd, EPOLL_CTL_ADD, fd, byref(ev))
        if ret == -1:
            raise OSError(errno.value, 'epoll_ctl failed')

    def modify(self, fd, readable, writable):
        ev = EpollEvent()
        ev.data.fd = fd
        events = 0
        if readable:
            events |= EPOLLIN
        if writable:
            events |= EPOLLOUT
        ev.events = events
        ret = epoll_ctl(self.efd, EPOLL_CTL_MOD, fd, byref(ev))
        if ret == -1:
            raise OSError(errno.value, 'epoll_ctl failed')

    def unregister(self, fd):
        ev = EpollEvent()
        ret = epoll_ctl(self.efd, EPOLL_CTL_DEL, fd, byref(ev))
        if ret == -1:
            raise OSError(errno.value, 'epoll_ctl failed')

    def __del__(self):
        efd = self.efd
        if efd >= 0:
            del self.efd
            close(efd)
    close = __del__
