"""Distributed, redundant and transactional storage for ZODB
"""

from setuptools import setup, find_packages
import os

classifiers = """\
Framework :: ZODB
Intended Audience :: Developers
License :: OSI Approved :: GNU General Public License (GPL)
Operating System :: POSIX :: Linux
Programming Language :: Python
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
"""

if not os.path.exists('mock.py'):
    import cStringIO, md5, urllib, zipfile
    mock_py = zipfile.ZipFile(cStringIO.StringIO(urllib.urlopen(
        'http://downloads.sf.net/sourceforge/python-mock/pythonmock-0.1.0.zip'
        ).read())).read('mock.py')
    if md5.md5(mock_py).hexdigest() != '79f42f390678e5195d9ce4ae43bd18ec':
        raise EnvironmentError("MD5 checksum mismatch downloading 'mock.py'")
    open('mock.py', 'w').write(mock_py)

extras_require = {
    'admin': [],
    'client': ['ZODB3'], # ZODB3 >= 3.10
    'ctl': [],
    'master': [],
    'storage-btree': ['ZODB3'],
    'storage-mysqldb': ['MySQL-python'],
}
extras_require['tests'] = ['zope.testing', 'psutil',
    'neoppod[%s]' % ', '.join(extras_require)]

setup(
    name = 'neoppod',
    version = '0.10.1',
    description = __doc__.strip(),
    author = 'NEOPPOD',
    author_email = 'neo-dev@erp5.org',
    url = 'http://www.neoppod.org/',
    license = 'GPL 2+',
    platforms = ["any"],
    classifiers=classifiers.splitlines(),
    long_description = ".. contents::\n\n" + open('README').read()
                     + "\n" + open('CHANGES').read(),
    packages = find_packages(),
    py_modules = ['mock'],
    entry_points = {
        'console_scripts': [
            # XXX: we'd like not to generate scripts for unwanted features
            # (eg. we don't want neotestrunner if nothing depends on tests)
            'neoadmin=neo.scripts.neoadmin:main',
            'neoctl=neo.scripts.neoctl:main',
            'neomaster=neo.scripts.neomaster:main',
            'neomigrate=neo.scripts.neomigrate:main',
            'neostorage=neo.scripts.neostorage:main',
            'neotestrunner=neo.scripts.runner:main',
            'neosimple=neo.scripts.simple:main',
            'stat_zodb=neo.tests.stat_zodb:main',
        ],
    },
    # Raah!!! I wish I could write something like:
    #  install_requires = ['python>=2.5|ctypes'],
    extras_require = extras_require,
    package_data = {
        'neo.client': [
            'component.xml',
        ],
    },
    zip_safe = True,
)
