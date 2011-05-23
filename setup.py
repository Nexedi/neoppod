# WKRD: Official Python packaging and buildout are not designed to build
#       several binary packages from a single source one. This setup.py
#       contains several hacks to work around this horrible limitation, and
#       make it possible to develop several eggs in the same tree/repository:
#       - in development mode, it behaves somehow like a mono-package
#       - otherwise, sdist/bdist commands produces several packages

description = 'Distributed, redundant and transactional storage for ZODB'

setup_common = dict(
    version = '1.0a1',
    author = 'NEOPPOD',
    author_email = 'neo-dev@erp5.org',
    url = 'http://www.neoppod.org/',
    license = 'GPL 2+',
    install_requires = ['neoppod'],
    namespace_packages = ['neo', 'neo.scripts'],
    zip_safe = True,
)

import os, sys
from distutils.filelist import findall

try:
    setup_only, = [l[5:].strip() for l in open('PKG-INFO') if l[:5] == 'Name:']
except IOError:
    setup_only = None

def setup(name, packages=(), extra_files=(), _done=[], **kw):
    if setup_only and setup_only != name or \
       _done and ('--help' in sys.argv or '--help-commands' in sys.argv):
        return
    from distutils.command.build import build
    from setuptools import find_packages, setup
    from setuptools.command import egg_info, develop
    kw['packages'] = sum((isinstance(p, basestring) and [p] or
        list(p[:1]) + [p[0] + '.' + x for x in find_packages(*p[1:])]
        for p in packages), [])
    # monkey-patch to build package in different folders
    build_initialize_options = build.initialize_options
    def initialize_options(self):
        build_initialize_options(self)
        self.build_base = os.path.join(self.build_base, name)
    # per-package manifest, instead of walking files under revision control
    walk_revctrl = egg_info.walk_revctrl
    # create only 1 egg-link for buildout
    develop_finalize_options = develop.develop.finalize_options
    def finalize_options(self):
        develop_finalize_options(self)
        self.egg_link = os.devnull
    try:
        build.initialize_options = initialize_options
        egg_info.walk_revctrl = lambda *args, **kw: extra_files
        if _done:
            develop.develop.finalize_options = finalize_options
        setup(name = name, **dict(setup_common, **kw))
    finally:
        develop.develop.finalize_options = develop_finalize_options
        build.initialize_options = build_initialize_options
        egg_info.walk_revctrl = walk_revctrl
    _done.append(name)

setup(
    name = 'neoppod',
    description = description + ' - Common part',
    packages = ['neo', ('neo.lib', 'neo/lib')],
    # Raah!!! I wish I could write something like:
    #  install_requires = ['python>=2.5|ctypes']
    install_requires = (),
    namespace_packages = ['neo'],
    extra_files = ['TODO'],
)

setup(
    name = 'neoadmin',
    description = description + ' - Admin part',
    packages = ['neo', ('neo.admin', 'neo/admin')],
    py_modules = ['neo.scripts.neoadmin'],
    entry_points = {
        'console_scripts': [
            'neoadmin=neo.scripts.neoadmin:main',
        ],
    },
    extra_files = ['neo.conf'],
)

setup(
    name = 'neoclient',
    description = description + ' - Client part',
    packages = ['neo', ('neo.client', 'neo/client')],
    install_requires = ['neoppod', 'ZODB3 >= 3.9'],
    py_modules = ['neo.scripts.neomigrate'],
    entry_points = {
        'console_scripts': [
            'neomigrate=neo.scripts.neomigrate:main',
        ],
    },
    package_data = {
        'neo.client': [
            'component.xml',
        ],
    },
    extra_files = ['neo/client/component.xml'], # required for Python < 2.7
)

setup(
    name = 'neoctl',
    description = description + ' - Controller part',
    packages = ['neo', ('neo.neoctl', 'neo/neoctl')],
    py_modules = ['neo.scripts.neoctl'],
    entry_points = {
        'console_scripts': [
            'neoctl=neo.scripts.neoctl:main',
        ],
    },
)

setup(
    name = 'neomaster',
    description = description + ' - Master part',
    packages = ['neo', ('neo.master', 'neo/master')],
    py_modules = ['neo.scripts.neomaster'],
    entry_points = {
        'console_scripts': [
            'neomaster=neo.scripts.neomaster:main',
        ],
    },
    extra_files = ['neo.conf'],
)

setup(
    name = 'neostorage',
    description = description + ' - Storage part',
    packages = ['neo', ('neo.storage', 'neo/storage')],
    py_modules = ['neo.scripts.neostorage'],
    entry_points = {
        'console_scripts': [
            'neostorage=neo.scripts.neostorage:main',
        ],
    },
    extras_require = {
        'btree': ['ZODB3'],
        'mysqldb': ['MySQL-python'],
    },
    extra_files = ['neo.conf'],
)

if setup_only in ('neotests', None) and not os.path.exists('mock.py'):
    import cStringIO, md5, urllib, zipfile
    mock_py = zipfile.ZipFile(cStringIO.StringIO(urllib.urlopen(
      'http://downloads.sf.net/sourceforge/python-mock/pythonmock-0.1.0.zip'
      ).read())).read('mock.py')
    if md5.md5(mock_py).hexdigest() != '79f42f390678e5195d9ce4ae43bd18ec':
        raise EnvironmentError("MD5 checksum mismatch downloading 'mock.py'")
    open('mock.py', 'w').write(mock_py)

setup(
    name = 'neotests',
    description = description + ' - Testing part',
    packages = ['neo', ('neo.tests', 'neo/tests')],
    install_requires = [
        'neoadmin',
        'neoclient',
        'neoctl',
        'neomaster',
        'neostorage[btree, mysqldb]',
        'psutil',
        'zope.testing',
    ],
    py_modules = ['mock', 'neo.scripts.runner'],
    entry_points = {
        'console_scripts': [
            'neotestrunner=neo.scripts.runner:main',
        ],
    },
    extra_files = ['TESTS.txt'] + findall('tools'),
)
