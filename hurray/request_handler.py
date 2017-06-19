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

import msgpack

from hurray.msgpack_ext import encode as encode_msgpack
from hurray.protocol import (CMD_CREATE_DATABASE, CMD_RENAME_DATABASE,
                             CMD_DELETE_DATABASE,
                             CMD_USE_DATABASE, CMD_CREATE_GROUP,
                             CMD_REQUIRE_GROUP, CMD_CREATE_DATASET,
                             CMD_REQUIRE_DATASET, CMD_GET_FILESIZE,
                             CMD_GET_NODE, CMD_GET_KEYS, CMD_GET_TREE,
                             CMD_SLICE_DATASET, CMD_BROADCAST_DATASET,
                             CMD_ATTRIBUTES_GET, CMD_ATTRIBUTES_SET,
                             CMD_ATTRIBUTES_CONTAINS, CMD_ATTRIBUTES_KEYS,
                             CMD_KW_CMD, CMD_KW_ARGS, CMD_KW_DB,
                             CMD_KW_DB_RENAMETO, CMD_KW_OVERWRITE, CMD_KW_PATH,
                             CMD_KW_DATA, CMD_KW_KEY, CMD_KW_STATUS,
                             CMD_KW_SHAPE, CMD_KW_DTYPE,
                             RESPONSE_ATTRS_CONTAINS, RESPONSE_ATTRS_KEYS,
                             RESPONSE_NODE_KEYS, RESPONSE_NODE_TREE)
from hurray.server.log import app_log
from hurray.server.options import define, options
from hurray.status_codes import (FILE_EXISTS, OK, FILE_NOT_FOUND, GROUP_EXISTS,
                                 NODE_NOT_FOUND, DATASET_EXISTS, VALUE_ERROR,
                                 TYPE_ERROR, CREATED, UNKNOWN_COMMAND,
                                 MISSING_ARGUMENT, MISSING_DATA,
                                 INCOMPATIBLE_DATA, KEY_ERROR,
                                 INVALID_ARGUMENT, INTERNAL_SERVER_ERROR)
from .swmr import File, Group, Dataset

DATABASE_COMMANDS = (
    CMD_CREATE_DATABASE,
    CMD_RENAME_DATABASE,
    CMD_DELETE_DATABASE,
    CMD_USE_DATABASE,
    CMD_GET_FILESIZE,
)

NODE_COMMANDS = (CMD_CREATE_GROUP,
                 CMD_REQUIRE_GROUP,
                 CMD_CREATE_DATASET,
                 CMD_REQUIRE_DATASET,
                 CMD_GET_NODE,
                 CMD_GET_KEYS,
                 CMD_GET_TREE,
                 CMD_SLICE_DATASET,
                 CMD_BROADCAST_DATASET,
                 CMD_ATTRIBUTES_GET,
                 CMD_ATTRIBUTES_SET,
                 CMD_ATTRIBUTES_CONTAINS,
                 CMD_ATTRIBUTES_KEYS)

define('base', default='~/hurray_data/', group='application',
       help="Location of hdf5 files")


def db_path(database):
    """
    Return the absolute path of the database based on the "base" option
    :param database: Name of hdf5 file
    :return: Absolute path of hdf5 file
    """
    absbase = os.path.abspath(os.path.expanduser(options.base))
    absfilepath = os.path.abspath(os.path.join(absbase, database))
    if not absfilepath.startswith(absbase):
        raise ValueError("File {} is not under base directory {}"
                         .format(absfilepath, absbase))

    return absfilepath


def db_exists(database):
    """
    Check if given database file exists
    :param database:
    :return:
    """
    path = db_path(database)
    app_log.debug('Checking if {} exists'.format(path))
    return os.path.isfile(path)


def response(status, data=None):
    """
    Args:
        status: status code of response
        data: NumPy array or Python object
    """
    resp = {
        CMD_KW_STATUS: status
    }
    if data is not None:
        resp["data"] = data
        # resp.update(data)

    # print("response (PID {}): {}".format(os.getpid(), resp))

    return msgpack.packb(resp, default=encode_msgpack, use_bin_type=True)


def handle_request(msg):
    """
    Process hurray message
    :param msg: Message dictionary with 'cmd' and 'args' keys
    :return: Msgpacked response as bytes
    """
    cmd = msg.get(CMD_KW_CMD, None)
    args = msg.get(CMD_KW_ARGS, {})
    data = msg.get(CMD_KW_DATA, None)

    app_log.debug('Process "%s" (%s)', cmd,
                  ', '.join(['%s=%s' % (k, v) for k, v in args.items()]))

    # return values
    status = OK
    data_response = None

    if cmd in DATABASE_COMMANDS:  # file related commands
        # Database name has to be defined
        if CMD_KW_DB not in args:
            return response(MISSING_ARGUMENT)
        db = args[CMD_KW_DB]
        if len(db) < 1:
            return response(INVALID_ARGUMENT)
        if cmd == CMD_CREATE_DATABASE:
            overwrite = args[CMD_KW_OVERWRITE]
            if db_exists(db) and not overwrite:
                status = FILE_EXISTS
            else:
                flags = "w" if overwrite else "w-"
                filepath = db_path(db)
                app_log.debug("Creating {} ...".format(filepath))
                # create sub-directories (if any)
                # note that db_path() guarantees that this is safe
                os.makedirs(os.path.split(filepath)[0], exist_ok=True)
                File(filepath, flags)
                status = CREATED
        elif cmd == CMD_RENAME_DATABASE:
            if not db_exists(db):
                status = FILE_NOT_FOUND
            else:
                f = File(db_path(db), "w")
                filepath_new = db_path(args[CMD_KW_DB_RENAMETO])
                f.rename(filepath_new)
                # we cannot return f because rename() is not "in place"
                f_renamed = File(filepath_new)
                data_response = f_renamed
        elif cmd == CMD_DELETE_DATABASE:
            if not db_exists(db):
                status = FILE_NOT_FOUND
            else:
                f = File(db_path(db), "w")
                f.delete()
        elif cmd == CMD_USE_DATABASE:
            if not db_exists(db):
                status = FILE_NOT_FOUND
        elif cmd == CMD_GET_FILESIZE:
            data_response = File(db_path(db), "r").filesize

    elif cmd in NODE_COMMANDS:  # Node related commands
        # Database name and path have to be defined
        if CMD_KW_DB not in args or CMD_KW_PATH not in args:
            return response(MISSING_ARGUMENT)

        db_name = args.get(CMD_KW_DB)
        # check if database exists
        if not db_exists(db_name):
            return response(FILE_NOT_FOUND)

        db = File(db_path(db_name), "r+")
        path = args[CMD_KW_PATH]

        if len(path) < 1:
            return response(INVALID_ARGUMENT)

        if cmd == CMD_CREATE_GROUP:
            if path in db:
                status = GROUP_EXISTS
            else:
                db.create_group(path)

        if cmd == CMD_REQUIRE_GROUP:
            db.require_group(path)

        elif cmd == CMD_CREATE_DATASET:
            if path in db:
                status = DATASET_EXISTS
            else:
                # shape=None, dtype=None, data=None
                shape = args.get(CMD_KW_SHAPE, None)
                dtype = args.get(CMD_KW_DTYPE, None)
                try:
                    dst = db.create_dataset(name=path, data=data, shape=shape,
                                            dtype=dtype)
                except TypeError as e:
                    return response(MISSING_DATA, data=str(e))
                except Exception as e:
                    return response(INTERNAL_SERVER_ERROR)
                data_response = dst

        elif cmd == CMD_REQUIRE_DATASET:
            # TODO raises error => https://github.com/meteotest/hurray/issues/5

            try:
                dst = db.require_dataset(name=path, data=data,
                                         **kwargs)
            except TypeError:
                return response(INCOMPATIBLE_DATA)
            data_response = dst

        else:  # Commands for existing nodes
            if path not in db:
                return response(NODE_NOT_FOUND)

            if cmd == CMD_GET_NODE:
                # let the msgpack encoder handle encoding of Groups/Datasets
                data_response = db[path]
            elif cmd == CMD_GET_KEYS:
                node = db[path]
                if isinstance(node, Group):
                    data_response = {
                        # without list() it does not work with py3 (returns a
                        # view on a closed hdf5 file)
                        RESPONSE_NODE_KEYS: list(node.keys())
                    }
                elif isinstance(node, Dataset):
                    return response(INVALID_ARGUMENT)
            elif cmd == CMD_GET_TREE:
                node = db[path]
                if isinstance(node, Group):
                    tree = node.tree()
                    data_response = {
                        RESPONSE_NODE_TREE: tree
                    }
                elif isinstance(node, Dataset):
                    return response(INVALID_ARGUMENT)
            elif cmd == CMD_SLICE_DATASET:
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                try:
                    data_response = db[path][args[CMD_KW_KEY]]
                except ValueError as ve:
                    status = VALUE_ERROR
                    app_log.debug('Invalid slice: %s', ve)

            elif cmd == CMD_BROADCAST_DATASET:
                if data is None:
                    return response(MISSING_DATA)
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                try:
                    db[path][args[CMD_KW_KEY]] = data
                except ValueError as ve:
                    status = VALUE_ERROR
                    app_log.debug('Invalid slice: %s', ve)
                except TypeError as te:
                    status = TYPE_ERROR
                    app_log.debug('Invalid broacdcast: %s', te)

            elif cmd == CMD_ATTRIBUTES_SET:
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                key = args[CMD_KW_KEY]
                if len(key) < 1:
                    return response(INVALID_ARGUMENT)
                if data is not None:
                    db[path].attrs[key] = data
                else:
                    return response(MISSING_DATA)
            elif cmd == CMD_ATTRIBUTES_GET:
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                try:
                    data_response = db[path].attrs[args[CMD_KW_KEY]]
                except KeyError as ke:
                    status = KEY_ERROR
                    app_log.debug('Invalid key: %s', ke)

            elif cmd == CMD_ATTRIBUTES_CONTAINS:
                if CMD_KW_KEY not in args:
                    return response(MISSING_ARGUMENT)
                data_response = {
                    RESPONSE_ATTRS_CONTAINS: args[CMD_KW_KEY] in db[path].attrs
                }
            elif cmd == CMD_ATTRIBUTES_KEYS:
                data_response = {
                    RESPONSE_ATTRS_KEYS: db[path].attrs.keys()
                }
    else:
        status = UNKNOWN_COMMAND

    return response(status, data_response)
