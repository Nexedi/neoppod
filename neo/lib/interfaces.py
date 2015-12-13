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

import inspect
from functools import wraps

def implements(obj, ignore=()):
    tobj = type(obj)
    ignore = set(ignore)
    not_implemented = []
    for name in dir(obj):
        method = getattr(obj if issubclass(tobj, type) or name in obj.__dict__
                             else tobj, name)
        if inspect.ismethod(method):
            try:
                method.__abstract__
                ignore.remove(name)
            except KeyError:
                not_implemented.append(name)
            except AttributeError:
                not_implemented.extend(func.__name__
                    for func in getattr(method, "__requires__", ())
                    if not hasattr(obj, func.__name__))
    assert not ignore, ignore
    assert not not_implemented, not_implemented
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
