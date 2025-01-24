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

from __future__ import print_function
try: # PY2
    from inspect import getargspec
except ImportError: # PY3
    from inspect import getfullargspec
    def getargspec(func):
        x = getfullargspec(func)
        return x.args, x.varargs, x.varkw, x.defaults
from neo import *

def check_signature(reference, function):
    # args, varargs, varkw, defaults
    A, B, C, D = getargspec(reference)
    a, b, c, d = getargspec(function)
    x = len(A) - len(a)
    if x < 0: # ignore extra default parameters
        if B or x + len(d) < 0:
            return False
        del a[x:]
        d = d[:x] or None
    elif x: # different signature
        return a == A[:-x] and (b or a and c) and (d or ()) == (D or ())[:-x]
    return a == A and (b or not B) and (c or not C) and d == D

def implements(obj, ignore=()):
    ignore = set(ignore)
    not_implemented = []
    wrong_signature = []
    if isinstance(obj, type):
        tobj = obj
    else:
        tobj = type(obj)
    mro = tobj.mro()
    mro.reverse()
    base = []
    for name in dir(obj):
        for x in mro:
            try:
                x = getattr(x, name)
                break
            except AttributeError:
                pass
        if hasattr(x, "__abstract__") or hasattr(x, "__requires__"):
            base.append((name, x if six.PY3 else x.__func__))
    try:
        while 1:
            name, func = base.pop()
            x = getattr(obj, name)
            if not isinstance(obj, type):
                try:
                    self = x.__self__
                except AttributeError:
                    continue
                if self.__class__ is not tobj:
                    continue
                x = x.__func__
            elif six.PY2:
                x = x.__func__
            if x is func:
                try:
                    x = func.__requires__
                except AttributeError:
                    try:
                        ignore.remove(name)
                    except KeyError:
                        not_implemented.append(name)
                else:
                    base.extend((x.__name__, x) for x in x)
            elif not check_signature(func, x):
                wrong_signature.append(name)
    except IndexError: # base empty
        assert not ignore, ignore
        assert not not_implemented, not_implemented
        assert not wrong_signature, wrong_signature
    return obj

def _stub(func):
    args, varargs, varkw, _ = getargspec(func)
    if varargs:
        args.append("*" + varargs)
    if varkw:
        args.append("**" + varkw)
    g = {}
    exec("def %s(%s): raise NotImplementedError\nf = %s" % (
        func.__name__, ",".join(args), func.__name__), g)
    return g['f']

def abstract(func):
    f = _stub(func)
    f.__abstract__ = 1
    f.__defaults__ = func.__defaults__
    f.__doc__ = func.__doc__
    return f

def requires(*args):
    for func in args:
        # Tolerate useless abstract decoration on required method (e.g. it
        # simplifies the implementation of a fallback decorator), but remove
        # marker since it does not need to be implemented if it's required
        # by a method that is overridden.
        try:
            del func.__abstract__
        except AttributeError:
            func.__code__ = _stub(func).__code__
    def decorator(func):
        func.__requires__ = args
        return func
    return decorator
