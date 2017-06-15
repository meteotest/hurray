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

import os
import logging
import signal
import struct
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing.util import _exit_function

import msgpack

from hurray.msgpack_ext import decode, encode
from hurray.protocol import MSG_LEN, PROTOCOL_VER
from hurray.request_handler import handle_request
from hurray.server import gen
from hurray.server import process
from hurray.server.ioloop import IOLoop
from hurray.server.iostream import StreamClosedError
from hurray.server.log import app_log
from hurray.server.netutil import bind_unix_socket, bind_sockets
from hurray.server.options import define, options, parse_config_file
from hurray.server.tcpserver import TCPServer
from hurray.status_codes import INTERNAL_SERVER_ERROR
from hurray.swmr import SWMR_SYNC, LOCK_STRATEGY_WRITER_PREFERENCE

SHUTDOWN_GRACE_PERIOD = 30

# command line arguments
define("host", default='localhost', group='application',
       help="IP address or hostname")
define("port", default=2222, group='application',
       help="TCP port to listen on (0 = do not listen on a port)")
define("socket", default=None, group='application',
       help="Unix socket path")
define("processes", default=0, group='application',
       help="Number of sub-processes (0 = detect the number of cores available"
            " on this machine)")
define("workers", default=1, group='application',
       help="Number of workers each sub-processes spawns")
define("locking", default=LOCK_STRATEGY_WRITER_PREFERENCE, group='application',
       help="File locking strategy:\nw = Writer preference\nn = No starving")
define("debug", default=0, group='application',
       help="Write debug information to stdout?")
define("config", type=str, help="path to config file",
       callback=lambda path: parse_config_file(path, final=False))


class HurrayServer(TCPServer):
    def __init__(self, *args, **kwargs):
        self.__workers = kwargs.pop('workers', 1)
        # ProcessPoolExecutor can't be initialized here.
        # The HurrayServer instances get forked and this leads to broken
        # process pools.
        self._pool = None
        super(HurrayServer, self).__init__(*args, **kwargs)

    @property
    def pool(self):
        if not self._pool:
            self._pool = ProcessPoolExecutor(max_workers=self.__workers)
        return self._pool

    def shutdown_pool(self):
        if self._pool:
            self._pool.shutdown()

    @gen.coroutine
    def handle_stream(self, stream, address):
        stream.set_nodelay(True)
        while True:
            try:
                # read protocol version
                protocol_ver = yield stream.read_bytes(MSG_LEN)
                protocol_ver = struct.unpack('>I', protocol_ver)[0]

                # Read message length (4 bytes) and unpack it into an integer
                raw_msg_length = yield stream.read_bytes(MSG_LEN)
                msg_length = struct.unpack('>I', raw_msg_length)[0]

                app_log.debug("Handle request (Protocol: v%d, Msg size: %d)",
                              protocol_ver, msg_length)

                data = yield stream.read_bytes(msg_length)
                msg = msgpack.unpackb(data, object_hook=decode,
                                      use_list=False, encoding='utf-8')

                try:
                    fut = self.pool.submit(handle_request, msg)
                    response = yield fut
                except Exception:
                    app_log.exception('Error in subprocess')
                    response = msgpack.packb({
                        'status': INTERNAL_SERVER_ERROR,
                    }, default=encode)

                rsp = struct.pack('>I', PROTOCOL_VER)
                # Prefix each message with a 4-byte length (network byte order)
                rsp += struct.pack('>I', len(response))
                rsp += response
                app_log.debug("Sending: {} bytes ...".format(len(rsp)))
                yield stream.write(rsp)
            except StreamClosedError:
                app_log.debug("Lost client at host %s", address)
                break
            except Exception:
                app_log.exception('Error while handling client connection')


def sig_handler(server, sig, frame):
    io_loop = IOLoop.instance()
    tid = process.task_id() or 0

    def stop_loop(deadline):
        now = time.time()
        if now < deadline and (io_loop._callbacks or io_loop._timeouts):
            io_loop.add_timeout(now + 1, stop_loop, deadline)
        else:
            io_loop.stop()
            server.shutdown_pool()
            logging.info('Task %d shutdown complete' % tid)

    def shutdown():
        logging.info('Stopping hurray server task %d' % tid)
        server.stop()
        stop_loop(time.time() + SHUTDOWN_GRACE_PERIOD)

    io_loop.add_callback_from_signal(shutdown)


def main():
    options.parse_command_line()

    if len(sys.argv) == 1:
        app_log.warning(
            "Warning: no config file specified, using the default config. "
            "In order to specify a config file use "
            "'hurray --config=/path/to/hurray.conf'")

    # check if base directory exists and is writable (TODO)
    absbase = os.path.abspath(os.path.expanduser(options.base))
    if not os.access(absbase, os.W_OK):
        app_log.error("base directory {} does not exist or is not writable!"
                      .format(absbase))
        sys.exit(1)

    SWMR_SYNC.set_strategy(options.locking)

    server = HurrayServer(workers=options.workers)

    sockets = []

    if options.port != 0:
        sockets = bind_sockets(options.port, options.host)
        app_log.info("Listening on %s:%d", options.host, options.port)

    if options.socket:
        app_log.info("Listening on %s", options.socket)
        sockets.append(bind_unix_socket(options.socket))

    if len(sockets) < 1:
        app_log.error('Define a socket and/or a port > 0')
        return

    signal.signal(signal.SIGTERM, partial(sig_handler, server))
    signal.signal(signal.SIGINT, partial(sig_handler, server))

    # Note that it does not make much sense to start >1 (master) processes
    # because they implement an async event loop that creates worker processes
    # itself.
    server.start(options.processes)

    # deregister the multiprocessing exit handler for the forked children.
    # Otherwise they try to join the shared (parent) process manager
    # SWMRSyncManager.
    import atexit
    atexit.unregister(_exit_function)

    server.add_sockets(sockets)
    IOLoop.current().start()


if __name__ == "__main__":
    main()
