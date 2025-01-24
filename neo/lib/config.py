#
# Copyright (C) 2006-2019  Nexedi SA
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

import argparse, os, sys
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser
from functools import wraps
from neo import *

class _DefaultList(list):
    """
    Special list type for default values of 'append' argparse actions,
    so that the parser restarts from an empty list when the option is
    used on the command-line.
    """

    def __copy__(self):
        return []

class _Required(object):

    def __init__(self, *args):
        self._option_list, self._name = args

    def __nonzero__(self):
        with_required = self._option_list._with_required
        return with_required is not None and self._name not in with_required

class _Option(object):

    multiple = False

    def __init__(self, *args, **kw):
        if len(args) > 1:
            self.short, self.name = args
        else:
            self.name, = args
        self.__dict__.update(kw)

    def _asArgparse(self, parser, option_list):
        kw = self._argument_kw()
        args = ['--' + self.name]
        try:
            args.insert(0, '-' + self.short)
        except AttributeError:
            pass
        kw['help'] = self.help
        action = parser.add_argument(*args, **kw)
        if action.required:
            assert not hasattr(self, 'default')
            action.required = _Required(option_list, self.name)

    def fromConfigFile(self, cfg, section):
        value = cfg.get(section, self.name.replace('-', '_'))
        if self.multiple:
            return [self(value)
                for value in value.splitlines()
                if value]
        return self(value)

    @staticmethod
    def parse(value):
        return value

class BoolOption(_Option):

    def _argument_kw(self):
        return {'action': 'store_true'}

    def __call__(self, value):
        return value

    def fromConfigFile(self, cfg, section):
        return cfg.getboolean(section, self.name.replace('-', '_'))

class Option(_Option):

    @property
    def __name__(self):
        return self.type.__name__

    def _argument_kw(self):
        kw = {'type': self}
        for x in 'default', 'metavar', 'required', 'choices':
            try:
                kw[x] = getattr(self, x)
            except AttributeError:
                pass
        if self.multiple:
            kw['action'] = 'append'
            default = kw.get('default')
            if default:
              kw['default'] = _DefaultList(default)
        return kw

    @staticmethod
    def type(value):
        if value:
            return value
        raise argparse.ArgumentTypeError('value is empty')

    def __call__(self, value):
        return self.type(value)

class OptionGroup(object):

    def __init__(self, description=None):
        self.description = description
        self._options = []

    def _asArgparse(self, parser, option_list):
        g = parser.add_argument_group(self.description)
        for option in self._options:
            option._asArgparse(g, option_list)

    def set_defaults(self, **kw):
        option_dict = self.getOptionDict()
        for k, v in six.iteritems(kw):
            option_dict[k].default = v

    def getOptionDict(self):
        option_dict = {}
        for option in self._options:
            if isinstance(option, OptionGroup):
                option_dict.update(option.getOptionDict())
            else:
                option_dict[option.name.replace('-', '_')] = option
        return option_dict

    def __call__(self, *args, **kw):
        self._options.append(Option(*args, **kw))

    def __option_type(t):
        return wraps(t)(lambda self, *args, **kw: self(type=t, *args, **kw))

    float = __option_type(float)
    int   = __option_type(int)
    path  = __option_type(os.path.expanduser)

    def bool(self, *args, **kw):
        self._options.append(BoolOption(*args, **kw))

class Argument(Option):

    def _asArgparse(self, parser, option_list):
        kw = {'help': self.help, 'type': self}
        for x in 'default', 'metavar', 'nargs', 'choices':
            try:
                kw[x] = getattr(self, x)
            except AttributeError:
                pass
        parser.add_argument(self.name, **kw)

class OptionList(OptionGroup):

    _with_required = None

    def argument(self, *args, **kw):
        self._options.append(Argument(*args, **kw))

    def group(self, description):
        group = OptionGroup(description)
        self._options.append(group)
        return group

    def parse(self, argv=None):
        parser = argparse.ArgumentParser(description=self.description,
            argument_default=argparse.SUPPRESS,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        for option in self._options:
            option._asArgparse(parser, self)
        _format_help = parser.format_help
        def format_help():
            self._with_required = ()
            try:
                return _format_help()
            finally:
                del self._with_required
        parser.format_help = format_help
        if argv is None:
            argv = sys.argv[1:]
        args = parser.parse_args(argv)
        option_dict = self.getOptionDict()
        try:
            config_file = args.file
        except AttributeError:
            d = ()
        else:
            cfg = ConfigParser()
            cfg.read(config_file)
            section = args.section
            d = {}
            for name in cfg.options(section):
                try:
                    option = option_dict[name]
                except KeyError:
                    continue
                d[name] = option.fromConfigFile(cfg, section)
            parser.set_defaults(**d)
        self._with_required = d
        try:
            args = parser.parse_args(argv)
        finally:
            del self._with_required
        return {name: option.parse(getattr(args, name))
            for name, option in six.iteritems(option_dict)
            if hasattr(args, name)}
