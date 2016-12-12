#!/usr/bin/env python
import sys

from setuptools import setup, find_packages

if sys.version_info < (2, 6):
    raise Exception("This package requires Python 2.6 or higher.")


def read_release_version():
    """Read the version from the file ``RELEASE-VERSION``."""
    with open("RELEASE-VERSION", "r") as f:
        return f.readline().strip()

url = 'https://github.com/bdeeney/PikaChewie'
version = read_release_version()

setup(
    name='PikaChewie',
    version=version,
    description='A pika-based RabbitMQ publisher-consumer framework',
    long_description='''PikaChewie is your pika co-pilot, providing RabbitMQ
        messaging tools with bandoliers included.''',
    author='Bryan Deeney',
    author_email='rennybot@pobox.com',
    packages=find_packages(exclude=['tests.*', 'tests']),
    url=url,
    download_url='{url}/tarball/{version}'.format(url=url, version=version),
    license='BSD',
    test_suite='nose.collector',
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'pika',
        'simplejson',
        'tornado',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Communications',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
