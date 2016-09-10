from __future__ import absolute_import

import os
import struct
from concurrent.futures import ProcessPoolExecutor

import msgpack
from h5pyswmr import File, Group, Dataset

from hurray.const import FILE_EXISTS, OK, FILE_NOTFOUND, NOT_IMPLEMENTED, INTERNAL_SERVER_ERROR, GROUP_EXISTS, \
    NODE_NOTFOUND, DATASET_EXISTS, VALUE_ERROR, TYPE_ERROR
from hurray.msgpack_numpy import decode_np_array, encode_np_array
from hurray.server import gen
from hurray.server.ioloop import IOLoop
from hurray.server.iostream import StreamClosedError
from hurray.server.log import app_log
from hurray.server.netutil import bind_unix_socket
from hurray.server.options import define, options
from hurray.server.tcpserver import TCPServer

MSG_LEN = 4
PROTOCOL_VER = 1
DATABASE_CMDS = (b'create_group',
                 b'get_node',
                 b'create_dataset',
                 b'slice_dataset',
                 b'broadcast_dataset',
                 b'attrs_setitem',
                 b'attrs_getitem',
                 b'attrs_contains',
                 b'attrs_keys')

define("host", default='localhost', group='application', help="IP address or hostname")
define("port", default=2222, group='application', help="TCP port to listen on")
define("socket", default=None, group='application', help="Unix socket path")
define("base", default='.', group='application', help="Database files location")


def dc(v):
    return v.decode(encoding='UTF-8') if v and isinstance(v, bytes) else v


class HurrayServer(TCPServer):
    @staticmethod
    def db_path(database):
        return os.path.abspath(os.path.join(options.base, database))

    @staticmethod
    def db_exists(database):
        path = HurrayServer.db_path(dc(database))
        app_log.debug('Check database %s', path)
        return os.path.isfile(path)

    @staticmethod
    def process(msg):
        cmd = msg[b'cmd']
        args = msg[b'args']

        app_log.debug('Process "%s" (%s)', dc(cmd),
                      ", ".join(["%s=%s" % (dc(k), dc(v)) for k, v in args.items()]))

        response = {
            'status': OK
        }

        if cmd == b'create_db':
            if HurrayServer.db_exists(args[b'name']):
                response['status'] = FILE_EXISTS
            else:
                File(HurrayServer.db_path(dc(args[b'name'])), "w-")
        elif cmd == b'connect_db':
            if not HurrayServer.db_exists(args[b'name']):
                response['status'] = FILE_NOTFOUND
        elif cmd in DATABASE_CMDS:
            db_name = args.get(b'db')
            # check if database exists
            if not HurrayServer.db_exists(db_name):
                response['status'] = FILE_NOTFOUND
            db = File(HurrayServer.db_path(dc(db_name)), "r+")
            path = dc(args[b'path'])

            if cmd == b'get_node':
                if path not in db:
                    response['status'] = NODE_NOTFOUND
                else:
                    node = db[path]
                    if isinstance(node, Group):
                        response['nodetype'] = 'group'
                    elif isinstance(node, Dataset):
                        response['nodetype'] = 'dataset'
                        response['shape'] = node.shape
                        response['dtype'] = str(node.dtype)

            elif cmd == b'slice_dataset':
                if path not in db:
                    response['status'] = NODE_NOTFOUND
                else:
                    try:
                        response['data'] = db[path][args[b'key']]
                    except ValueError as ve:
                        response['status'] = VALUE_ERROR
                        app_log.debug('Invalid slice: %s', ve)

            elif cmd == b'broadcast_dataset':
                if path not in db:
                    response['status'] = NODE_NOTFOUND
                else:
                    # todo: what should we return?
                    try:
                        db[path][args[b'key']] = msg[b'arr']
                        response['data'] = msg[b'arr']
                    except ValueError as ve:
                        response['status'] = VALUE_ERROR
                        app_log.debug('Invalid slice: %s', ve)
                    except TypeError as te:
                        response['status'] = TYPE_ERROR
                        app_log.debug('Invalid broacdcast: %s', te)

            elif cmd == b'attrs_setitem':
                if path not in db:
                    response['status'] = NODE_NOTFOUND
                else:
                    if b'value' in args:
                        data = dc(args[b'value'])
                    else:
                        data = msg[b'arr']
                    db[path].attrs[dc(args[b'key'])] = data

            elif cmd == b'attrs_getitem':
                if path not in db:
                    response['status'] = NODE_NOTFOUND
                else:
                    response['data'] = db[path].attrs[dc(args[b'key'])]

            elif cmd == b'attrs_contains':
                if path not in db:
                    response['status'] = NODE_NOTFOUND
                else:
                    response['contains'] = dc(args[b'key']) in db[path].attrs

            elif cmd == b'attrs_keys':
                if path not in db:
                    response['status'] = NODE_NOTFOUND
                else:
                    response['keys'] = db[path].attrs.keys()

            elif cmd == b'create_group':
                if path in db:
                    response['status'] = GROUP_EXISTS
                else:
                    db.create_group(path)

            elif cmd == b'create_dataset':
                if path in db:
                    response['status'] = DATASET_EXISTS
                else:
                    db.create_dataset(name=path, data=msg[b'arr'])
        else:
            response['status'] = NOT_IMPLEMENTED

        return msgpack.packb(response, default=encode_np_array)

    @gen.coroutine
    def handle_stream(self, stream, address):
        pool = ProcessPoolExecutor(max_workers=1)
        while True:
            try:
                # read protocol version
                protocol_ver = yield stream.read_bytes(MSG_LEN)
                protocol_ver = struct.unpack('>I', protocol_ver)[0]

                # Read message length (4 bytes) and unpack it into an integer
                raw_msg_length = yield stream.read_bytes(MSG_LEN)
                msg_length = struct.unpack('>I', raw_msg_length)[0]

                app_log.debug("Handle request (Protocol: v%d, Msg size: %d)", protocol_ver, msg_length)

                data = yield stream.read_bytes(msg_length)
                msg = msgpack.unpackb(data, object_hook=decode_np_array, use_list=False)

                try:
                    fut = pool.submit(HurrayServer.process, msg)
                    response = yield fut
                    # response = HurrayServer.process(msg)
                except Exception:
                    app_log.exception('Error in subprocess')
                    response = msgpack.packb({
                        'status': INTERNAL_SERVER_ERROR,
                    }, default=encode_np_array)

                yield stream.write(struct.pack('>I', PROTOCOL_VER))

                # Prefix each message with a 4-byte length (network byte order)
                yield stream.write(struct.pack('>I', len(response)))

                yield stream.write(response)
            except StreamClosedError:
                app_log.info("Lost client at host %s", address)
                break
            except Exception:
                app_log.exception('Error while handling client connection')
        pool.shutdown()


def main():
    options.parse_command_line()
    server = HurrayServer()

    if options.socket:
        print("Listening on %s" % options.socket)
        unix_socket = bind_unix_socket(options.socket)
        server.add_socket(unix_socket)
    else:
        server.bind(options.port, options.host)
        print("Listening on %s:%d" % (options.host, options.port))
        server.start(0)  # Forks multiple sub-processes

    IOLoop.current().start()


if __name__ == "__main__":
    main()
