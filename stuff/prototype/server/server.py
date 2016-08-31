"""
hfive server

see https://github.com/KeepSafe/aiohttp/blob/master/examples/mpsrv.py
"""

from multiprocessing import Process  # , Pipe
import os
import socket
import signal
import asyncio
import struct
import json

import numpy as np
from numpy.lib.format import header_data_from_array_1_0

from hfive.dispatch import dispatch
from hfive.config import DATADIR
from hfive.locking import clear_semaphores


# 4 bytes are used to encode message lengths
PROTOCOL_VER = 1
MSG_LEN = 4


def decode_slice_json(dct):
    """
    object_hook for JSONDecoder that takes care of slice() objects encoded
    as dictionaries
    """
    if '__slice__' in dct:
        return slice(dct['start'], dct['stop'], dct['step'])
    else:
        return dct


class Worker:
    """
    An instance of a server.
    """

    def __init__(self, sock):
        """
        Args:
            sock: socket object
        """
        self.sock = sock

    def start(self):
        """
        start server process
        """
        self.loop = loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def stop():
            self.loop.stop()
            os._exit(0)
        loop.add_signal_handler(signal.SIGINT, stop)

        coro = asyncio.start_server(Worker.connection_handler, sock=self.sock)
        srv = loop.run_until_complete(coro)

        x = srv.sockets[0]
        host, port = x.getsockname()
        print('Starting server PID {} on {}:{}...'
              .format(os.getpid(), host, port))

        loop.run_forever()
        os._exit(0)
        # TODO close server?
        # server.close()
        # loop.run_until_complete(server.wait_closed())
        # loop.close()

    @asyncio.coroutine
    def connection_handler(reader, writer):
        """
        Runs for each client connected.
        ``reader`` is a StreamReader object, ``writer`` is a
        StreamWriter object.
        """
        print("Connection established! (server PID: {})".format(os.getpid()))

        cmd, args, arr = yield from Worker.recv(reader)

        print('cmd: {}'.format(cmd))
        print('args: {}'.format(args))
        result, arr = dispatch(cmd, args, arr)
        print("result from dispatch: {}".format(result))

        Worker.send(writer, result, arr)

    def recv(reader):
        """
        Receive and decode a message.

        Args:
            reader: StreamReader object

        Returns:
            Tuple (command, args, array), where array is either a numpy array
            or None.
        """
        # read protocol version
        protocol_ver = yield from reader.readexactly(MSG_LEN)
        print(protocol_ver)

        # Read message length (4 bytes) and unpack it into an integer
        raw_msglen = yield from reader.readexactly(MSG_LEN)
        no_bytes = struct.unpack('>I', raw_msglen)[0]
        print("message size: {}".format(no_bytes))
        # receive data block of indicated size
        alldata = yield from reader.readexactly(no_bytes)
        # TODO alldata may be empty

        data = json.loads(alldata.decode('utf8'), object_hook=decode_slice_json)

        print("**************")
        print("data:", data)
        cmd = data['cmd']
        args = data['args']

        # decode array data
        if 'array_meta' in data:  # check if an array is expected
            array_len = yield from reader.readexactly(MSG_LEN)
            array_len_bytes = struct.unpack('>I', array_len)[0]
            try:
                dtype = data['array_meta']['descr']
                shape = data['array_meta']['shape']
                fortran_order = data['array_meta']['fortran_order']
            except KeyError:
                # TODO
                arr = None
            else:
                arr_data = yield from reader.readexactly(array_len_bytes)
                print("array length on server: {} vs {}"
                      .format(array_len_bytes, len(arr_data)))
                assert(array_len_bytes == len(arr_data))
                # TODO use np.frombuffer()
                arr = np.fromstring(arr_data, dtype=np.dtype(dtype))
                print("*********************")
                print(arr.shape, shape)
                arr.shape = shape
                if fortran_order:
                    arr.shape = shape[::-1]
                    arr = arr.transpose()
        else:
            arr = None

        return cmd, args, arr

    def send(writer, result, arr=None):
        """
        Encode and send message.

        Args:
            writer: StreamWriter object
            result: dict containing result of request
            arr: numpy array or None
        """
        # send protocol version
        writer.write(struct.pack('>I', PROTOCOL_VER))

        # encode data
        data = result.copy()
        if arr is not None:
            data['array_meta'] = header_data_from_array_1_0(arr)

        msg = json.dumps(data).encode('utf8')
        print("Sending {} bytes...".format(len(msg)))
        # Prefix each message with a 4-byte length (network byte order)
        msg_prefixed = struct.pack('>I', len(msg)) + msg
        writer.write(msg_prefixed)
        # send array data (if any)
        if arr is not None:
            arr_str = arr.tostring()
            arr_len = struct.pack('>I', len(arr_str))
            print("sending array data...")
            writer.write(arr_len)
            writer.write(arr_str)


class SuperVisor:
    """
    Spawns a new server process and keeps track of it.
    """

    def __init__(self, loop, sock):
        """
        Args:
            loop: event loop instance
            sock: socket object
        """
        self._started = False
        self.loop = loop
        self.sock = sock
        self.pid = os.getpid()
        self.__spawn()

    def __spawn(self):
        """
        Spawn server process
        """
        assert not self._started
        self._started = True

        def f():
            # cleanup after fork
            asyncio.set_event_loop(None)
            # setup process
            process = Worker(self.sock)
            process.start()

        p = Process(target=f, args=[])
        p.start()


class Server():
    """
    Main server class. Spawns supervisors/workers.
    """

    def __init__(self):
        self.supervisors = []

    def start(self, host, port, no_workers=1):
        """
        Start ``no_workers`` server instances.

        Args:
            host: host name of IP
            port: TCP port number
            no_workers: desired number of server processes
        """
        # clean up all semaphores
        dbs = self.get_dbs()
        print("Found the following hdf5 databases:")
        print("\n".join(dbs))
        print("")
        clear_semaphores(dbs)

        # bind socket
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(1024)
        sock.setblocking(False)

        loop = asyncio.get_event_loop()

        # start processes
        print("Starting {} server processes on {}:{}..."
              .format(no_workers, host, port))
        for _ in range(no_workers):
            self.supervisors.append(SuperVisor(loop, sock))

        loop.add_signal_handler(signal.SIGINT, lambda: loop.stop())
        loop.run_forever()

    def get_dbs(self):
        """
        Returns a list of all available databases (without .h5 extension).
        """
        dbs = [os.path.splitext(f)[0] for f in os.listdir(DATADIR)
               if f.endswith(".h5")]

        return dbs
