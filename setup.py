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
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
"""

mock = 'neo/tests/mock.py'
if not os.path.exists(mock):
    import cStringIO, hashlib, subprocess, urllib, zipfile
    x = 'pythonmock-0.1.0.zip'
    try:
        x = subprocess.check_output(('git', 'cat-file', 'blob', x))
    except (OSError, subprocess.CalledProcessError):
        x = urllib.urlopen(
            'http://downloads.sf.net/sourceforge/python-mock/' + x).read()
    mock_py = zipfile.ZipFile(cStringIO.StringIO(x)).read('mock.py')
    if (hashlib.sha256(mock_py).hexdigest() !=
        'c6ed26e4312ed82160016637a9b6f8baa71cf31a67c555d44045a1ef1d60d1bc'):
        raise EnvironmentError("SHA checksum mismatch downloading 'mock.py'")
    open(mock, 'w').write(mock_py)

zodb_require = ['ZODB3>=3.10dev']

extras_require = {
    'admin': [],
    'client': zodb_require,
    'ctl': [],
    'master': [],
    'storage-sqlite': [],
    'storage-mysqldb': ['mysqlclient'],
    'storage-importer': zodb_require,
}
extras_require['tests'] = ['coverage', 'zope.testing', 'psutil>=2',
    'neoppod[%s]' % ', '.join(extras_require)]

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
    version = '1.8.1',
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
            'stat_zodb=neo.tests.stat_zodb:main',
        ],
        'zodburi.resolvers': [
            'neo = neo.client.zodburi:resolve_uri [client]',
        ],
    },
    extras_require = extras_require,
    package_data = {
        'neo.client': [
            'component.xml',
        ],
    },
    zip_safe = True,
)
