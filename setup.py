#!/usr/bin/env python
#
# Copyright (c) 2016, Meteotest
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


import sys

try:
    # Use setuptools if available, for install_requires (among other things).
    import setuptools
    from setuptools import setup
except ImportError:
    setuptools = None
    from distutils.core import setup

kwargs = {}

version = "1.0.dev1"

with open('README.rst') as f:
    kwargs['long_description'] = f.read()

if setuptools is not None:
    # If setuptools is not available, you're on your own for dependencies.
    install_requires = []
    if sys.version_info < (2, 7):
        # Only needed indirectly, for singledispatch.
        install_requires.append('ordereddict')
    if sys.version_info < (2, 7, 9):
        install_requires.append('backports.ssl_match_hostname')
    if sys.version_info < (3, 4):
        install_requires.append('singledispatch')
        # Certifi is also optional on 2.7.9+, although making our dependencies
        # conditional on micro version numbers seems like a bad idea
        # until we have more declarative metadata.
        install_requires.append('certifi')
    if sys.version_info < (3, 5):
        install_requires.append('backports_abc>=0.4')
    kwargs['install_requires'] = install_requires

setup(
    name="hurray",
    version=version,
    packages=["hurray", "hurray.test", "hurray.platform"],
    package_data={
        # data files need to be listed both here (which determines what gets
        # installed) and in MANIFEST.in (which determines what gets included
        # in the sdist tarball)
        "hurray.test": [],
    },
    url="https://github.com/meteotest/hurray/",
    author='Meteotest',
    author_email='remo.goetschi@meteotest.ch',
    maintainer='Reto Aebersold',
    maintainer_email='aeby@substyle.ch',
    license='MIT',
    description="Hurray is a Python server exposing an API to access hdf5 files",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5'
    ],
    **kwargs
)
