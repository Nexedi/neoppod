from setuptools import setup, find_packages

setup(name='neo',
    version='0.1.0',
    description='Distributed, redundant and transactional storage for ZODB',
    author='NEOPPOD',
    author_email='neo-dev@erp5.org',
    url='http://www.neoppod.org/',
    license="GPL 2",
    packages=find_packages(),
    package_dir={'neo': 'neo'},
    install_requires=[
        'ZODB3',
    ],
    extras_require={
        'storage': ['MySQL-python'],
        'test': ['MySQL-python', 'mock'],
    },
    entry_points={
        'console_scripts': [
            'neoadmin=neo.scripts.neoadmin:main',
            'neoctl=neo.scripts.neoctl:main',
            'neomaster=neo.scripts.neomaster:main',
            'neomigrate=neo.scripts.neomigrate:main',
            'neostorage=neo.scripts.neostorage:main',
        ],
    },
    zip_safe=False,
)

