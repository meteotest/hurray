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

"""
Simple function wrappers for read and write operations using the configured
strategy.  The acquisition of the read and write locks (start_read,
start_write) as well as the read and write operation itself _must_ be wrapped
with try/finally. Otherwise the corresponding locks cannot be
decremented/released if program execution ends, e.g., while performing a read
or write operation (because of a SIGTERM signal, for example).
"""

from functools import wraps

from .exithandler import handle_exit
from .lock import SWMR_SYNC


def reader(f):
    """
    Decorates methods reading a shared resource
    """

    @wraps(f)
    def func_wrapper(self, *args, **kwargs):
        """
        Wraps reading functions.
        """
        with handle_exit(append=True):
            try:
                SWMR_SYNC.start_read(self.file)
                result = f(self, *args, **kwargs)  # critical section
                return result
            finally:
                SWMR_SYNC.end_read(self.file)

    return func_wrapper


def writer(f):
    """
    Decorates methods writing to a shared resource
    """

    @wraps(f)
    def func_wrapper(self, *args, **kwargs):
        """
        Wraps writing functions.
        """
        with handle_exit(append=True):
            try:
                SWMR_SYNC.start_write(self.file)
                return_val = f(self, *args, **kwargs)
                return return_val
            finally:
                SWMR_SYNC.end_write(self.file)

    return func_wrapper
