# The use of ast is convoluted, and the result quite verbose,
# but that remains simpler than writing a parser from scratch.
import ast, os
from contextlib import contextmanager
from neo.lib.protocol import Packet, Enum

array = list, set, tuple
item = Enum.Item

class _ast(object):
    def __getattr__(self, k):
        v = lambda *args: getattr(ast, k)(lineno=0, col_offset=0, *args)
        setattr(self, k, v)
        return v
_ast = _ast()

class parseArgument(ast.NodeTransformer):

    def visit_UnaryOp(self, node):
        assert isinstance(node.op, ast.USub)
        return _ast.Call(_ast.Name('option', ast.Load()),
            [self.visit(node.operand)], [], None, None)

    def visit_Name(self, node):
        return _ast.Str(node.id.replace('_', '?'))

parseArgument = parseArgument().visit

class Argument(object):

    merge = True
    type = ''
    option = False

    @classmethod
    def load(cls, arg):
        arg = ast.parse(arg.rstrip()
            .replace('?(', '-(').replace('?[', '-[').replace('?{', '-{')
            .replace('?', '_').replace('[]', '[""]')
            .replace('{:', '{"":').replace(':}', ':""}'),
            mode="eval")
        x = arg.body
        name = x.func.id
        arg.body = parseArgument(_ast.Tuple(x.args, ast.Load()))
        return name, cls._load(eval(compile(arg, '', mode="eval"),
                                    {'option': cls._option}))

    @classmethod
    def _load(cls, arg):
        t = type(arg)
        if t is cls:
            return arg
        x = object.__new__(cls)
        if t is tuple:
            x.type = map(cls._load, arg)
        elif t is list:
            x.type = cls._load(*arg),
        elif t is dict:
            (k, v), = arg.iteritems()
            x.type = cls._load(k), cls._load(v)
        else:
            if arg.startswith('?'):
                arg = arg[1:]
                x.option = True
            x.type = arg
        return x

    @classmethod
    def _option(cls, arg):
        arg = cls._load(arg)
        arg.option = True
        return arg

    @classmethod
    def _merge(cls, args):
        if args:
            x, = {cls(x) for x in args}
            return x
        return object.__new__(cls)

    def __init__(self, arg, root=False):
        if arg is None:
            self.option = True
        elif isinstance(arg, tuple) and (root or len(arg) > 1):
            self.type = map(self.__class__, arg)
        elif isinstance(arg, array):
            self.type = self._merge(arg),
        elif isinstance(arg, dict):
            self.type = self._merge(arg), self._merge(arg.values())
        else:
            self.type = (('p64' if len(arg) == 8 else
                'bin')          if isinstance(arg, bytes)   else
                arg._enum._name if isinstance(arg, item)    else
                'str'           if isinstance(arg, unicode) else
                type(arg).__name__)

    def __repr__(self):
        x = self.type
        if type(x) is tuple:
            x = ('[%s]' if len(x) == 1 else '{%s:%s}') % x
        elif type(x) is list:
            x = '(%s)' % ','.join(map(repr, x))
        return '?' + x if self.option else x

    def __hash__(self):
        return 0

    def __eq__(self, other):
        x = self.type
        y = other.type
        if x and y and x != 'any':
            # Since we don't whether an array is fixed-size record of
            # heterogeneous values or a collection of homogeneous values,
            # we end up with the following complicated heuristic.
            t = type(x)
            if t is tuple:
                if len(x) == 1 and type(y) is list:
                    z = set(x)
                    z.update(y)
                    if len(z) == 1:
                        x = y = tuple(z)
                        if self.merge:
                            self.type = x
            elif t is list:
                if type(y) is tuple and len(y) == 1:
                    z = set(y)
                    z.update(x)
                    if len(z) == 1:
                        x = y = tuple(z)
                        if self.merge:
                            self.type = x
                        t = tuple
            elif t is str is type(y) and {x, y}.issuperset(('bin', 'p64')):
                x = y = 'bin'
                if self.merge:
                    self.type = x
            if not (t is type(y) and (t is not tuple or
                                      len(x) == len(y)) and x == y):
                if not self.merge:
                    return False
                self.type = 'any'
        if self.merge:
            if not x:
                self.type = y
            if not self.option:
                self.option = other.option
        elif y and not x or other.option and not self.option:
            return False
        return True

class FrozenArgument(Argument):
    merge = False

@contextmanager
def protocolChecker(dump):
    x = 'Packet(p64,?[(bin,{int:})],{:?(?,[])},?{?:float})'
    assert x == '%s%r' % Argument.load(x)
    assert not (FrozenArgument([]) == Argument([0]))

    path = os.path.join(os.path.dirname(__file__), 'protocol')
    if dump:
        import threading
        from multiprocessing import Lock
        lock = Lock()
        schema = {}
        pid = os.getpid()
        r, w = os.pipe()
        def _check(name, arg):
            try:
                schema[name] == arg
            except KeyError:
                schema[name] = arg
        def check(name, args):
            arg = Argument(args, True)
            if pid == os.getpid():
                _check(name, arg)
            else:
                with lock:
                    os.write(w, '%s%r\n' % (name, arg))
        def check_thread(r):
            for x in os.fdopen(r):
                _check(*Argument.load(x))
        check_thread = threading.Thread(target=check_thread, args=(r,))
        check_thread.daemon = True
        check_thread.start()
    else:
        with open(path) as p:
            p.readline()
            schema = dict(map(FrozenArgument.load, p))
        def check(name, args):
            arg = Argument(args, True)
            if not (None is not schema.get(name) == arg):
                raise Exception('invalid packet: %s%r' % (name, arg))
        w = None
    Packet_encode = Packet.__dict__['encode']
    def encode(packet):
        check(type(packet).__name__, packet._args)
        return Packet_encode(packet)
    Packet.encode = encode
    try:
        yield
    finally:
        Packet.encode = Packet_encode
        if w:
            os.close(w)
            check_thread.join()
    if dump:
        with open(path, 'w') as p:
            p.write('# generated by running the whole test suite with -p\n')
            for x in sorted(schema.iteritems()):
                p.write('%s%r\n' % x)
