#!/usr/bin/env python
import sys

from setuptools import setup, find_packages

if sys.version_info < (2, 6):
    raise Exception("This package requires Python 2.6 or higher.")


def read_release_version():
    """Read the version from the file ``RELEASE-VERSION``."""
    with open("RELEASE-VERSION", "r") as f:
        return f.readline().strip()

setup(
    name='PikaChewie',
    description='A pika-based RabbitMQ publisher-consumer framework',
    long_description='''PikaChewie is your pika co-pilot, providing RabbitMQ
        messaging tools with bandoliers included.''',
    author='Bryan Deeney',
    author_email='rennybot@pobox.com',
    packages=find_packages(exclude=['tests.*', 'tests']),
    test_suite='nose.collector',
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'pika>=0.9.13',
        'simplejson',
        'tornado',
    ],
    version=read_release_version(),
)
