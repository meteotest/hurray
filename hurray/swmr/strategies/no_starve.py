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
The third readers-writers problem
https://www.rfc1149.net/blog/2011/01/07/the-third-readers-writers-problem/
"""

from multiprocessing import Semaphore

_rw_locks = {}
_locker = Semaphore(1)


def _get_locks(name):
    with _locker:
        return _rw_locks.setdefault(name, {
            'access': Semaphore(1),
            'readers': Semaphore(1),
            'order': Semaphore(1),
            'rds': 0
        })


def start_read(name):
    locks = _get_locks(name)
    locks['order'].acquire()

    locks['readers'].acquire()
    if locks['rds'] == 0:
        locks['access'].acquire()
    locks['rds'] += 1

    locks['order'].release()
    locks['readers'].release()


def end_read(name):
    locks = _get_locks(name)
    with locks['readers']:
        locks['rds'] -= 1
        if locks['rds'] == 0:
            locks['access'].release()


def start_write(name):
    locks = _get_locks(name)
    with locks['order']:
        locks['access'].acquire()


def end_write(name):
    locks = _get_locks(name)
    locks['access'].release()
