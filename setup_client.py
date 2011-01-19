from setuptools import setup, find_packages

setup(name='neoclient',

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
     'neo.scripts.neomigrate',
      ],

    packages=['neo.client','neo.client.handlers'],    
    
    package_dir={
      'neo':'neo',
    },

    namespace_packages=['neo','neo.client'],    
    
    install_requires=[
      'neo',
      'ZODB3',
    ],

    zip_safe=False,
)

