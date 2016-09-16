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
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from __future__ import absolute_import

import os

import msgpack
from h5pyswmr import File, Group, Dataset
from hurray.msgpack_ext import encode_np_array
from hurray.server.log import app_log
from hurray.server.options import define, options
from hurray.status_codes import FILE_EXISTS, OK, FILE_NOT_FOUND, GROUP_EXISTS, \
    NODE_NOT_FOUND, DATASET_EXISTS, VALUE_ERROR, TYPE_ERROR, CREATED, UNKNOWN_COMMAND, MISSING_ARGUMENT, MISSING_DATA, \
    KEY_ERROR

MSG_LEN = 4
PROTOCOL_VER = 1

CMD_KW_CMD = 'cmd'
CMD_KW_ARGS = 'args'
CMD_KW_DATA = 'data'
CMD_KW_PATH = 'path'
CMD_KW_KEY = 'key'
CMD_KW_DB = 'db'
CMD_KW_STATUS = 'status'

CMD_CREATE_DATABASE = 'create_db'
CMD_CONNECT_DATABASE = 'connect_db'
CMD_CREATE_GROUP = 'create_group'
CMD_CREATE_DATASET = 'create_dataset'
CMD_GET_NODE = 'get_node'
CMD_SLICE_DATASET = 'slice_dataset'
CMD_BROADCAST_DATASET = 'broadcast_dataset'

CMD_ATTRIBUTES_GET = 'attrs_getitem'
CMD_ATTRIBUTES_SET = 'attrs_setitem'
CMD_ATTRIBUTES_CONTAINS = 'attrs_contains'
CMD_ATTRIBUTES_KEYS = 'attrs_keys'

RESPONSE_NODE_TYPE = 'nodetype'
RESPONSE_NODE_SHAPE = 'shape'
RESPONSE_NODE_DTYPE = 'dtype'
RESPONSE_ATTRS_CONTAINS = 'contains'
RESPONSE_ATTRS_KEYS = 'keys'
RESPONSE_DATA = 'data'

DATABASE_COMMANDS = (
    CMD_CREATE_DATABASE,
    CMD_CONNECT_DATABASE
)

NODE_COMMANDS = (CMD_CREATE_GROUP,
                 CMD_CREATE_DATASET,
                 CMD_GET_NODE,
                 CMD_SLICE_DATASET,
                 CMD_BROADCAST_DATASET,
                 CMD_ATTRIBUTES_GET,
                 CMD_ATTRIBUTES_SET,
                 CMD_ATTRIBUTES_CONTAINS,
                 CMD_ATTRIBUTES_KEYS)

NODE_TYPE_GROUP = 'group'
NODE_TYPE_DATASET = 'dataset'

define('base', default='.', group='application', help="Database files location")


def decode(value):
    """
    Return a string UTF-8 decoded from the given bytes.
    :param value: by
    :return: String
    """
    return value.decode(encoding='UTF-8') if value and isinstance(value, (bytes, bytearray)) else value


def db_path(database):
    """
    Return the absolute path of the database based on the "base" option
    :param database: Name of database file
    :return: Absolute path
    """
    return os.path.abspath(os.path.join(options.base, decode(database)))


def db_exists(database):
    """
    Check if given database file exists
    :param database:
    :return:
    """
    path = db_path(database)
    app_log.debug('Check database %s', path)
    return os.path.isfile(path)


def response(status, data=None):
    res = {
        CMD_KW_STATUS: status
    }
    if data:
        res.update(data)
    return msgpack.packb(res, default=encode_np_array, use_bin_type=True)


def handle_request(msg):
    """
    Process hurray message
    :param msg: Message dictionary with 'cmd' and 'args' keys
    :return: Msgpacked response as bytes
    """
    cmd = msg.get(CMD_KW_CMD, None)
    args = msg.get(CMD_KW_ARGS, {})

    app_log.debug('Process "%s" (%s)', decode(cmd),
                  ', '.join(['%s=%s' % (decode(k), decode(v)) for k, v in args.items()]))

    status = OK
    data = None

    if cmd in DATABASE_COMMANDS:  # Database related commands
        # Database name has to be defined
        if CMD_KW_DB not in args:
            return response(MISSING_ARGUMENT)
        db = args[CMD_KW_DB]
        if cmd == CMD_CREATE_DATABASE:
            if db_exists(db):
                status = FILE_EXISTS
            else:
                File(db_path(db), 'w-')
                status = CREATED
        elif cmd == CMD_CONNECT_DATABASE:
            if not db_exists(db):
                status = FILE_NOT_FOUND

    elif cmd in NODE_COMMANDS:  # Node related commands
        # Database name and path have to be defined
        if CMD_KW_DB not in args or CMD_KW_PATH not in args:
            return response(MISSING_ARGUMENT)

        db_name = args.get(CMD_KW_DB)
        # check if database exists
        if not db_exists(db_name):
            return response(FILE_NOT_FOUND)

        db = File(db_path(db_name), "r+")
        path = decode(args[CMD_KW_PATH])

        if cmd == CMD_CREATE_GROUP:
            if path in db:
                status = GROUP_EXISTS
            else:
                db.create_group(path)

        elif cmd == CMD_CREATE_DATASET:
            if path in db:
                status = DATASET_EXISTS
            else:
                if CMD_KW_DATA not in msg:
                    return response(MISSING_DATA)
                db.create_dataset(name=path, data=msg[CMD_KW_DATA])
        else:  # Commands for existing nodes
            if path not in db:
                return response(NODE_NOT_FOUND)

            if cmd == CMD_GET_NODE:
                node = db[path]
                if isinstance(node, Group):
                    data = {
                        RESPONSE_NODE_TYPE: NODE_TYPE_GROUP
                    }
                elif isinstance(node, Dataset):
                    data = {
                        RESPONSE_NODE_TYPE: NODE_TYPE_DATASET,
                        RESPONSE_NODE_SHAPE: node.shape,
                        RESPONSE_NODE_DTYPE: str(node.dtype)
                    }
            elif cmd == CMD_SLICE_DATASET:
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                try:
                    data = {
                        RESPONSE_DATA: db[path][args[CMD_KW_KEY]]
                    }
                except ValueError as ve:
                    status = VALUE_ERROR
                    app_log.debug('Invalid slice: %s', ve)

            elif cmd == CMD_BROADCAST_DATASET:
                if CMD_KW_DATA not in msg:
                    return response(MISSING_DATA)
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                try:
                    db[path][args[CMD_KW_KEY]] = msg[CMD_KW_DATA]
                except ValueError as ve:
                    status = VALUE_ERROR
                    app_log.debug('Invalid slice: %s', ve)
                except TypeError as te:
                    status = TYPE_ERROR
                    app_log.debug('Invalid broacdcast: %s', te)

            elif cmd == CMD_ATTRIBUTES_SET:
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                if CMD_KW_DATA in msg:
                    db[path].attrs[decode(args[CMD_KW_KEY])] = msg[CMD_KW_DATA]
                else:
                    return response(MISSING_DATA)
            elif cmd == CMD_ATTRIBUTES_GET:
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                try:
                    data = {
                        RESPONSE_DATA: db[path].attrs[decode(args[CMD_KW_KEY])]
                    }
                except KeyError as ke:
                    status = KEY_ERROR
                    app_log.debug('Invalid key: %s', ke)

            elif cmd == CMD_ATTRIBUTES_CONTAINS:
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                data = {
                    RESPONSE_ATTRS_CONTAINS: args[CMD_KW_KEY] in db[path].attrs
                }
            elif cmd == CMD_ATTRIBUTES_KEYS:
                data = {
                    RESPONSE_ATTRS_KEYS: db[path].attrs.keys()
                }
    else:
        status = UNKNOWN_COMMAND

    return response(status, data)
