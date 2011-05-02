from setuptools import setup, find_packages

setup(name='neotests',

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
      'neo.scripts.runner'
      ],

    packages=['neo.tests','neo.tests.functional','neo.tests.master','neo.tests.storage','neo.tests.zodb','neo.tests.client'],

    package_dir={
      'neo':'neo',
    },

    namespace_packages=['neo','neo.scripts'],

    install_requires=[
      'neo',
      'neoadmin',
      'neostorage',
      'neoclient',
      'neomaster',
      'neoctl',
      'mock'
    ],

    entry_points = {
        'console_scripts': [
                'neotestrunner=neo.scripts.runner:main',
        ],
    },
    zip_safe=False,
)

