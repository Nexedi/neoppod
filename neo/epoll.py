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

class epoll_data(Union):
    _fields_ = [("ptr", c_void_p),
                ("fd", c_int),
                ("u32", c_uint32),
                ("u64", c_uint64)]

class epoll_event(Structure):
    _fields_ = [("events", c_uint32),
                ("data", epoll_data)]

class Epoll(object):
    efd = -1

    def __init__(self):
        self.efd = epoll_create(10)
        if self.efd == -1:
            raise OSError(errno.value, 'epoll_create failed')

        self.maxevents = 1024 # XXX arbitrary
        epoll_event_array = epoll_event * self.maxevents
        self.events = epoll_event_array()

    def poll(self, timeout = 1):
        timeout *= 1000
        timeout = int(timeout)
        while 1:
            n = epoll_wait(self.efd, byref(self.events), self.maxevents, 
                           timeout)
            if n == -1:
                e = errno.value
                if e in (EINTR, EAGAIN):
                    continue
                else:
                    raise OSError(e, 'epoll_wait failed')
            else:
                readable_fd_list = []
                writable_fd_list = []
                for i in xrange(n):
                    ev = self.events[i]
                    if ev.events & (EPOLLIN | EPOLLERR | EPOLLHUP):
                        readable_fd_list.append(int(ev.data.fd))
                    elif ev.events & (EPOLLOUT | EPOLLERR | EPOLLHUP):
                        writable_fd_list.append(int(ev.data.fd))
                return readable_fd_list, writable_fd_list

    def register(self, fd):
        ev = epoll_event()
        ev.data.fd = fd
        ret = epoll_ctl(self.efd, EPOLL_CTL_ADD, fd, byref(ev))
        if ret == -1:
            raise OSError(errno.value, 'epoll_ctl failed')

    def modify(self, fd, readable, writable):
        ev = epoll_event()
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
        ev = epoll_event()
        ret = epoll_ctl(self.efd, EPOLL_CTL_DEL, fd, byref(ev))
        if ret == -1:
            raise OSError(errno.value, 'epoll_ctl failed')

    def __del__(self):
        if self.efd >= 0:
            close(self.efd)
