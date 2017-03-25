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
A server process manager providing different locking strategies to processes accessing a shared resource.
See strategies for concrete implementations.
"""

from multiprocessing.managers import BaseManager

from .strategies import no_starve, writer_preference, LOCK_STRATEGY_NO_STARVE, LOCK_STRATEGY_WRITER_PREFERENCE


class SWMRSync(object):
    def __init__(self):
        self.__strategy = writer_preference

    def set_strategy(self, strategy):
        if strategy == LOCK_STRATEGY_NO_STARVE:
            self.__strategy = no_starve
        elif strategy == LOCK_STRATEGY_WRITER_PREFERENCE:
            self.__strategy = writer_preference
        else:
            self.__strategy = None
            raise Exception('Unknown locking strategy %s' % strategy)

    def start_read(self, name):
        return self.__strategy.start_read(name)

    def end_read(self, name):
        return self.__strategy.end_read(name)

    def start_write(self, name):
        return self.__strategy.start_write(name)

    def end_write(self, name):
        return self.__strategy.end_write(name)


class SWMRSyncManager(BaseManager):
    pass


SWMRSyncManager.register('SWMRSync', SWMRSync)


def start_sync_manager():
    """
    Start a server process which holds the SWMRSync object.
    Other processes can manipulate (mainly acquire and release locks) it using a proxy 
    :return: A SWMRSync proxy
    """
    manager = SWMRSyncManager()
    manager.start()
    return manager.SWMRSync()


# All forked children have to access SWMRSync object using the SWMR_SYNC proxy.
# It is important that this module is imported before the child processes are forked
# to ensure that the manager is started by the parent process.
SWMR_SYNC = start_sync_manager()
