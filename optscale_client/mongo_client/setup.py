#!/usr/bin/env python
from setuptools import setup


setup(name='mongo-client',
      description='OptScale MongoDB Client Helper',
      author='Hystax',
      url='http://hystax.com',
      author_email='info@hystax.com',
      package_dir={'mongo_client': ''},
      install_requires=['pymongo>=3.0'],
      packages=['mongo_client']
      )
