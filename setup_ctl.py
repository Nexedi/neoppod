from setuptools import setup, find_packages

setup(name='neoctl',

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
      'neo.scripts.neoctl'
      ],

    packages=['neo.neoctl'],

    package_dir={
      'neo':'neo',
    },

    namespace_packages=['neo','neo.scripts'],

    install_requires=[
      'neo',
    ],

    entry_points = {
        'console_scripts': [
          'neoctl=neo.scripts.neoctl:main',
        ],
    },
    zip_safe=False,
)

