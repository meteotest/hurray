"""
Hfive Python Client, DB connection interface
"""

import asyncio
import json
import struct

import numpy as np
from numpy.lib.format import header_data_from_array_1_0

from hfivepy.nodes import Group
from hfivepy.const import FILE_EXISTS, FILE_NOTFOUND
from hfivepy.jsonencoder import SliceEncoder


# 4 bytes are used to encode message lengths
MSG_LEN = 4
PROTOCOL_VER = 1


def _recv(reader):
    """
    Receive and decode message

    Args:
        reader: StreamReader object

    Returns:
        Tuple (result, array), where result is a dict and array is either a
        numpy array or None.
    """
    # read protocol version
    protocol_ver = yield from reader.readexactly(MSG_LEN)
    protocol_ver = struct.unpack('>I', protocol_ver)[0]
    print("protocol version:", protocol_ver)

    # Read message length (4 bytes) and unpack it into an integer
    raw_msglen = yield from reader.readexactly(MSG_LEN)
    no_bytes = struct.unpack('>I', raw_msglen)[0]
    # print("message size: {}".format(no_bytes))

    result_json = yield from reader.readexactly(no_bytes)
    # decode message
    result = json.loads(result_json.decode('utf8'))

    print(result)

    # decode array data
    if 'array_meta' in result:  # check if an array is expected
        print("reading length of array data...", flush=True)
        array_len = yield from reader.readexactly(MSG_LEN)
        array_len_bytes = struct.unpack('>I', array_len)[0]
        print("array length: ", array_len_bytes)
        try:
            dtype = result['array_meta']['descr']
            shape = result['array_meta']['shape']
            fortran_order = result['array_meta']['fortran_order']
        except KeyError:
            # TODO raise exception
            arr = None
        else:
            arr_data = yield from reader.readexactly(array_len_bytes)
            # TODO use np.frombuffer()
            arr = np.fromstring(arr_data, dtype=np.dtype(dtype))
            arr.shape = shape
            if fortran_order:
                arr.shape = shape[::-1]
                arr = arr.transpose()
    else:
        arr = None

    return (result, arr)


class Connection:
    """
    Connection to an hfive server and database/file
    """

    def __init__(self, host, port, db=None):
        """
        Args:
            host: host name or IP address
            port: TCP port
            db: name of database or None
        """
        self.__loop = asyncio.get_event_loop()
        self.__host = host
        self.__port = port
        self.__db = db
        # TODO start handshake with server

    def __enter__(self):
        """
        simple context manager (so we can use 'with Connection() as conn:')
        """
        return self

    def __exit__(self, type, value, tb):
        pass

    def create_db(self, name):
        """
        Create a database / hdf5 file

        Args:
            name: str, name of the database

        Returns:
            None

        Raises:
            OSError if db already exists
        """
        result, _ = self.send_rcv('create_db', {'name': name})

        if result['statuscode'] == FILE_EXISTS:
            raise OSError('db exists')

    def connect_db(self, dbname):
        """
        Connect to database

        Args:
            dbname: str, name of the database

        Returns:
            An instance of the Group class

        Raises:
            ValueError if ``dbname`` does not exist
        """
        result, _ = self.send_rcv('connect_db', {'name': dbname})

        if result['statuscode'] == FILE_NOTFOUND:
            raise ValueError('db not found')
        else:
            self.__db = dbname
            return Group(self, '/')

    def test(self):
        """
        A simple test function
        """
        args = {
            'bla': 3
        }
        arr = np.array([3.4, 5.645, 5.6])
        result, arr = self.send_rcv('test', args, arr)

        return arr

    def send_rcv(self, cmd, args, arr=None):
        """
        Process a request to the server

        Args:
            cmd: command
            args: command arguments
            arr: numpy array or None

        Returns:
            Tuple (result, array)
        """
        if 'db' not in args:
            args['db'] = self.__db
        send_rcv_coroutine = self.__send_rcv(cmd, args, arr)
        result, array = self.__loop.run_until_complete(send_rcv_coroutine)

        return result, array

    @asyncio.coroutine
    def __send_rcv(self, cmd, args, arr):
        """
        """
        # reader: StreamReader object, writer: StreamWriter object
        reader, writer = yield from asyncio.open_connection(self.__host,
                                                            self.__port)

        data = {
            'cmd': cmd,
            'args': args,
        }
        if arr is not None:
            data['array_meta'] = header_data_from_array_1_0(arr)

        data_ser = json.dumps(data, cls=SliceEncoder).encode('utf8')

        # print("Sending {} bytes...".format(msg_len))
        # Prefix message with protocol version
        writer.write(struct.pack('>I', PROTOCOL_VER))
        # Prefix each message with a 4-byte length (network byte order)
        writer.write(struct.pack('>I', len(data_ser)))
        # send metadata
        writer.write(data_ser)
        # send numpy arrays
        # TODO don't copy arrays, use the buffer protocol
        # https://zeromq.github.io/pyzmq/serialization.html
        # https://github.com/mila-udem/fuel/blob/master/fuel/server.py
        # numpy array into Gnu R:
        # http://dirk.eddelbuettel.com/blog/2012/06/30/
        # http://stackoverflow.com/questions/28341785/copying-bytes-in-python-from-numpy-array-into-string-or-bytearray
        # http://blog.enthought.com/python/numpy/fast-numpyprotobuf-deserialization-example/#.VaOPcZOlilM
        if arr is not None:
            arr_str = arr.tostring()
            print("array length: {}".format(len(arr_str)))
            arr_len = struct.pack('>I', len(arr_str))
            writer.write(arr_len)
            writer.write(arr_str)

        # receive answer from server
        print("receiving answer from server...", flush=True)
        result, array = yield from _recv(reader)
        writer.close()

        return result, array

    @property
    def db(self):
        """
        wrapper
        """
        return self.__db


def connect(host='localhost', port=2222, db=None):
    """
    Creates and returns a database connection object.

    Args:
        host: str, hostname or IP address
        port: int, TCP port
        db: database name

    Returns:
        An instance of the Connection class
    """
    return Connection(host, port, db)
