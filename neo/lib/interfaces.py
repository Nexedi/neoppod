#
# Copyright (C) 2015-2016  Nexedi SA
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

import inspect

def check_signature(reference, function):
    # args, varargs, varkw, defaults
    A, B, C, D = inspect.getargspec(reference)
    a, b, c, d = inspect.getargspec(function)
    x = len(A) - len(a)
    if x < 0: # ignore extra default parameters
        if x + len(d) < 0:
            return False
        del a[x:]
        d = d[:x] or None
    elif x: # different signature
        # We have no need yet to support methods with default parameters.
        return a == A[:-x] and (b or a and c) and not (d or D)
    return a == A and b == B and c == C and d == D

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
            base.append((name, x.__func__))
    try:
        while 1:
            name, func = base.pop()
            x = getattr(obj, name)
            if x.im_class is tobj:
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

def _set_code(func):
    args, varargs, varkw, _ = inspect.getargspec(func)
    if varargs:
        args.append("*" + varargs)
    if varkw:
        args.append("**" + varkw)
    exec "def %s(%s): raise NotImplementedError\nf = %s" % (
        func.__name__, ",".join(args), func.__name__)
    func.func_code = f.func_code

def abstract(func):
    _set_code(func)
    func.__abstract__ = 1
    return func

def requires(*args):
    for func in args:
        _set_code(func)
    def decorator(func):
        func.__requires__ = args
        return func
    return decorator
