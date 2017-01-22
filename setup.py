# Copyright (c) 2016, Meteotest
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of Meteotest nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import sys

import io

from hurray import __version__

try:
    # Use setuptools if available, for install_requires (among other things).
    import setuptools
    from setuptools import setup, find_packages
except ImportError:
    setuptools = None
    from distutils.core import setup, find_packages

kwargs = {}

with io.open('README.rst', encoding='utf-8') as f:
    kwargs['long_description'] = f.read()

if setuptools is not None:
    # If setuptools is not available, you're on your own for dependencies.
    install_requires = [
        'numpy==1.12.0',
        'msgpack-python==0.4.8',
        'h5py==2.6',
        'redis==2.10.5',
    ]

    if sys.version_info < (2, 7):
        # Only needed indirectly, for singledispatch.
        install_requires.append('ordereddict')
    if sys.version_info < (2, 7, 9):
        install_requires.append('backports.ssl_match_hostname')
    if sys.version_info < (3, 2):
        install_requires.append('futures')
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
    version=__version__,
    # packages=["hurray"],
    packages=find_packages(),
    url="https://github.com/meteotest/hurray/",
    author='Meteotest',
    author_email='remo.goetschi@meteotest.ch',
    maintainer='Reto Aebersold',
    maintainer_email='aeby@substyle.ch',
    license='BSD',
    description="Hurray is a Python server exposing an API to access hdf5 "
    "files",
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    test_suite='tests.get_tests',
    entry_points={
        'console_scripts': [
            'hurray = hurray.__main__:main'
        ]
    },
    **kwargs
)
