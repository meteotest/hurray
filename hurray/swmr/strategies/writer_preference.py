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
The algorithm implemented is "Problem 2" in the following paper:
http://cs.nyu.edu/~lerner/spring10/MCP-S10-Read04-ReadersWriters.pdf
"""

from multiprocessing import Semaphore

_rw_locks = {}
_locker = Semaphore(1)


def _get_locks(name):
    with _locker:
        return _rw_locks.setdefault(name, {
            'mutex1': Semaphore(1),
            'mutex2': Semaphore(1),
            'mutex3': Semaphore(1),
            'r': Semaphore(1),
            'w': Semaphore(1),
            'rcnt': 0,
            'wcnt': 0
        })


def start_read(name):
    locks = _get_locks(name)
    with locks['mutex3']:
        with locks['r']:
            with locks['mutex1']:
                locks['rcnt'] += 1
                if locks['rcnt'] == 1:
                    locks['w'].acquire()


def end_read(name):
    locks = _get_locks(name)
    with locks['mutex1']:
        locks['rcnt'] -= 1
        if locks['rcnt'] == 0:
            locks['w'].release()


def start_write(name):
    locks = _get_locks(name)
    with locks['mutex2']:
        locks['wcnt'] += 1
        if locks['wcnt'] == 1:
            locks['r'].acquire()
        locks['w'].acquire()


def end_write(name):
    locks = _get_locks(name)
    locks['w'].release()
    with locks['mutex2']:
        locks['wcnt'] -= 1
        if locks['wcnt'] == 0:
            locks['r'].release()
