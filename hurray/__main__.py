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

from __future__ import absolute_import

import logging
import struct
from concurrent.futures import ProcessPoolExecutor

import msgpack

from hurray.h5swmr.sync import clear_locks
from hurray.msgpack_ext import decode, encode
from hurray.request_handler import handle_request
from hurray.server import gen
from hurray.server.ioloop import IOLoop
from hurray.server.iostream import StreamClosedError
from hurray.server.log import app_log
from hurray.server.netutil import bind_unix_socket, bind_sockets
from hurray.server.options import define, options
from hurray.server.tcpserver import TCPServer
from hurray.status_codes import INTERNAL_SERVER_ERROR

MSG_LEN = 4
PROTOCOL_VER = 1

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
define("debug", default=0, group='application',
       help="Write debug information to stdout?")


class HurrayServer(TCPServer):
    @gen.coroutine
    def handle_stream(self, stream, address):
        # this creates one worker process for each newly created connection
        stream.set_nodelay(True)
        pool = ProcessPoolExecutor(max_workers=1)
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
                    fut = pool.submit(handle_request, msg)
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
                yield stream.write(rsp)
            except StreamClosedError:
                app_log.info("Lost client at host %s", address)
                break
            except Exception:
                app_log.exception('Error while handling client connection')
        pool.shutdown()


def main():
    options.parse_command_line()

    debug = bool(options.debug)
    if debug:
        app_log.setLevel(logging.DEBUG)
        app_log.debug("debug mode")

    server = HurrayServer()

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

    clear_locks()
    # Note that it does not make much sense to start >1 (master) processes
    # because they implement an async event loop that creates worker processes
    # itself.
    server.start(options.processes)
    server.add_sockets(sockets)
    IOLoop.current().start()


if __name__ == "__main__":
    main()
