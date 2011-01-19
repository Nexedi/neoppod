from setuptools import setup, find_packages

setup(name='neostorage',

    version=
      '0.1.0',
    description=
      'Distributed, redundant and transactional storage for ZODB-Admin part',
    author=
      'NEOPPOD',
    author_email=
      'neo-dev@erp5.org',
    url=
      'http://www.neoppod.org/',
    license=
      "GPL 2",

    py_modules=[
      'neo.protocol',
      'neo.scripts.neostorage'
      ],

    packages=['neo.storage','neo.storage.database','neo.storage.handlers'],
    
    package_dir={
      'neo':'neo',
    },

    namespace_packages=['neo','neo.scripts'],    
    
    install_requires=[
      'neo',
      'MySQL-python',
    ],

    entry_points = {
        'console_scripts': [
          'neostorage=neo.scripts.neostorage:main',
        ],
    }, 
    zip_safe=False,
)

