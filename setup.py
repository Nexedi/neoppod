"""Distributed, redundant and transactional storage for ZODB
"""

from setuptools import setup, find_packages
import os

classifiers = """\
Framework :: ZODB
Intended Audience :: Developers
License :: OSI Approved :: GNU General Public License (GPL)
Operating System :: POSIX :: Linux
Programming Language :: Python :: 2.7
Programming Language :: Python :: Implementation :: CPython
Programming Language :: Python :: Implementation :: PyPy
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
"""

def get3rdParty(name, tag, url, h, extract=lambda content, name: content):
    path = 'neo/tests/' + name
    if os.path.exists(path):
        return
    import hashlib, subprocess, urllib
    try:
        x = subprocess.check_output(('git', 'cat-file', 'blob', tag))
    except (OSError, subprocess.CalledProcessError):
        x = urllib.urlopen(url).read()
    x = extract(x, name)
    if hashlib.sha256(x).hexdigest() != h:
        raise EnvironmentError("SHA checksum mismatch downloading '%s'" % name)
    with open(path, 'wb') as f:
        f.write(x)

def unzip(content, name):
    import io, zipfile
    return zipfile.ZipFile(io.BytesIO(content)).read(name)

x = 'pythonmock-0.1.0.zip'
get3rdParty('mock.py', x,
    'http://downloads.sf.net/sourceforge/python-mock/' + x,
    'c6ed26e4312ed82160016637a9b6f8baa71cf31a67c555d44045a1ef1d60d1bc',
    unzip)

x = 'ConflictFree.py'
get3rdParty(x, '3rdparty/' + x, 'https://lab.nexedi.com/nexedi/erp5'
    '/raw/14b0fcdcc31c5791646f9590678ca028f5d221f5/product/ERP5Type/' + x,
    'abb7970856540fd02150edd1fa9a3a3e8d0074ec526ab189684ef7ea9b41825f')

zodb_require = ['ZODB>=4.4.5']

extras_require = {
    'admin': [],
    'client': zodb_require,
    'ctl': [],
    'master': [],
    'reflink': zodb_require,
    'storage-sqlite': [],
    'storage-mysqldb': ['mysqlclient'],
    'storage-pymysql': ['PyMySQL'],
    'storage-importer': zodb_require + ['setproctitle'],
}
def self_require(*extra):
    # PY3: The latest version of pip for Python 2 does not support recursive
    #      optional dependencies so we need to expand manually.
    req = set()
    for extra in extra:
        req.update(extras_require[extra])
    return sorted(req)
extras_require['tests'] = ['coverage', 'zope.testing', 'psutil>=2',
    'mock', # ZODB test dependency
] + self_require(*extras_require)
extras_require['stress'] = ['NetfilterQueue', 'gevent',
    'cython-zstd', # recommended (log rotation)
] + self_require('tests')

try:
    from docutils.core import publish_string
except ImportError:
    pass
else:
    from distutils.dist import DistributionMetadata
    _get_long_description = DistributionMetadata.get_long_description.im_func
    def get_long_description(self):
        r = _get_long_description(self)
        DistributionMetadata.get_long_description = _get_long_description
        publish_string(r, enable_exit_status=True, settings_overrides={
            'exit_status_level': 0})
        return r
    DistributionMetadata.get_long_description = get_long_description

setup(
    name = 'neoppod',
    version = '1.12.0',
    description = __doc__.strip(),
    author = 'Nexedi SA',
    author_email = 'neo-dev@erp5.org',
    url = 'https://neo.nexedi.com/',
    license = 'GPL 2+',
    platforms = ["any"],
    classifiers=classifiers.splitlines(),
    long_description = ".. contents::\n\n" + open('README.rst').read()
                     + "\n" + open('CHANGELOG.rst').read(),
    packages = find_packages(),
    entry_points = {
        'console_scripts': [
            # XXX: we'd like not to generate scripts for unwanted features
            # (eg. we don't want neotestrunner if nothing depends on tests)
            'neoadmin=neo.scripts.neoadmin:main',
            'neoctl=neo.scripts.neoctl:main',
            'neolog=neo.scripts.neolog:main',
            'neomaster=neo.scripts.neomaster:main',
            'neomigrate=neo.scripts.neomigrate:main',
            'neostorage=neo.scripts.neostorage:main',
            'neotestrunner=neo.scripts.runner:main',
            'neosimple=neo.scripts.simple:main',
            'reflink=neo.scripts.reflink:main',
            'stat_zodb=neo.tests.stat_zodb:main',
        ],
        'zodburi.resolvers': [
            'neo = neo.client.zodburi:resolve_uri [client]',
            'neos = neo.client.zodburi:resolve_uri [client]',
        ],
    },
    install_requires = [
        'msgpack>=0.5.6,<1',
        'python-dateutil', # neolog --from
        ],
    extras_require = extras_require,
    package_data = {
        'neo.client': [
            'component.xml',
        ],
    },
    zip_safe = True,
)
