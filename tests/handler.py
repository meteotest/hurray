import os
import shutil
import tempfile
import unittest

import msgpack
import numpy as np
from hurray.msgpack_ext import decode
from hurray.protocol import (CMD_CREATE_DATABASE, CMD_USE_DATABASE,
                             CMD_KW_CMD, CMD_KW_DB, CMD_KW_ARGS, CMD_KW_STATUS,
                             CMD_CREATE_GROUP, CMD_KW_PATH, CMD_CREATE_DATASET,
                             CMD_KW_DATA, CMD_GET_NODE, NODE_TYPE_GROUP,
                             RESPONSE_NODE_TYPE, NODE_TYPE_DATASET,
                             RESPONSE_NODE_SHAPE, RESPONSE_NODE_DTYPE,
                             CMD_SLICE_DATASET, CMD_KW_KEY, RESPONSE_DATA,
                             CMD_BROADCAST_DATASET, CMD_ATTRIBUTES_SET,
                             CMD_ATTRIBUTES_GET, CMD_ATTRIBUTES_CONTAINS,
                             RESPONSE_ATTRS_CONTAINS, CMD_ATTRIBUTES_KEYS,
                             RESPONSE_ATTRS_KEYS)
from hurray.request_handler import handle_request
from hurray.server.options import options
from hurray.status_codes import (UNKNOWN_COMMAND, MISSING_ARGUMENT, CREATED,
                                 FILE_NOT_FOUND, OK, GROUP_EXISTS,
                                 MISSING_DATA, DATASET_EXISTS, NODE_NOT_FOUND,
                                 VALUE_ERROR, TYPE_ERROR, KEY_ERROR,
                                 INVALID_ARGUMENT)
from numpy.testing import assert_array_equal


def unpack(data):
    """
    Unpack msgpacked data
    :param data:
    :return:
    """
    return msgpack.unpackb(data, object_hook=decode, use_list=False,
                           encoding='utf-8')


class RequestHandlerTestCase(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        # Set base to tmp dir
        options.base = self.test_dir

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def create_db(self, name):
        cmd = {
            CMD_KW_CMD: CMD_CREATE_DATABASE,
            CMD_KW_ARGS: {CMD_KW_DB: name}
        }
        return unpack(handle_request(cmd))

    def create_grp(self, db, path):
        cmd = {
            CMD_KW_CMD: CMD_CREATE_GROUP,
            CMD_KW_ARGS: {
                CMD_KW_DB: db,
                CMD_KW_PATH: path
            }
        }
        return unpack(handle_request(cmd))

    def create_ds(self, db, path, data):
        cmd = {
            CMD_KW_CMD: CMD_CREATE_DATASET,
            CMD_KW_ARGS: {
                CMD_KW_DB: db,
                CMD_KW_PATH: path
            },
            CMD_KW_DATA: data
        }
        return unpack(handle_request(cmd))

    def test_no_cmd(self):
        response = unpack(handle_request({}))
        self.assertEqual(response[CMD_KW_STATUS], UNKNOWN_COMMAND)

    def test_create_database(self):
        cmd = {
            CMD_KW_CMD: CMD_CREATE_DATABASE,
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        db_name = ''
        response = self.create_db(db_name)
        self.assertEqual(response[CMD_KW_STATUS], INVALID_ARGUMENT)

        db_name = 'test.h5'
        response = self.create_db(db_name)
        self.assertEqual(response[CMD_KW_STATUS], CREATED)
        self.assertTrue(os.path.isfile(os.path.join(self.test_dir, db_name)))

    def test_use_database(self):
        cmd = {
            CMD_KW_CMD: CMD_USE_DATABASE,
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        db_name = 'test.h5'
        cmd[CMD_KW_ARGS] = {CMD_KW_DB: db_name}
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], FILE_NOT_FOUND)

        self.create_db(db_name)
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], OK)

    def test_create_group(self):
        db_name = 'test.h5'
        self.create_db(db_name)

        cmd = {
            CMD_KW_CMD: CMD_CREATE_GROUP,
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        cmd[CMD_KW_ARGS] = {CMD_KW_DB: db_name}
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        cmd[CMD_KW_ARGS][CMD_KW_PATH] = 'mygrp'
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], OK)

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], GROUP_EXISTS)

    def test_create_dataset(self):
        db_name = 'test.h5'
        self.create_db(db_name)

        cmd = {
            CMD_KW_CMD: CMD_CREATE_DATASET,
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        cmd[CMD_KW_ARGS] = {CMD_KW_DB: db_name}
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        cmd[CMD_KW_ARGS][CMD_KW_PATH] = 'myds'
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_DATA)

        cmd[CMD_KW_DATA] = (np.random.randint(0, 255, size=(5, 10))
                            .astype('uint8'))

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], OK)

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], DATASET_EXISTS)

    def test_get_node(self):
        db_name = 'test.h5'
        ds_name = 'testds'
        grp_name = 'testgrp'
        data = np.random.randint(0, 255, size=(5, 10)).astype('uint8')

        self.create_db(db_name)
        self.create_grp(db_name, grp_name)
        self.create_ds(db_name, ds_name, data)

        # get group
        cmd = {
            CMD_KW_CMD: CMD_GET_NODE,
            CMD_KW_ARGS: {
                CMD_KW_DB: db_name,
                CMD_KW_PATH: 'invalid'
            }
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], NODE_NOT_FOUND)

        cmd[CMD_KW_ARGS][CMD_KW_PATH] = grp_name  # now use existing group name

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], OK)
        self.assertEqual(response[CMD_KW_DATA][RESPONSE_NODE_TYPE],
                         NODE_TYPE_GROUP)

        # get dataset
        cmd = {
            CMD_KW_CMD: CMD_GET_NODE,
            CMD_KW_ARGS: {
                CMD_KW_DB: db_name,
                CMD_KW_PATH: ds_name
            }
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], OK)
        self.assertEqual(response[CMD_KW_DATA][RESPONSE_NODE_TYPE],
                         NODE_TYPE_DATASET)
        self.assertEqual(response[CMD_KW_DATA][RESPONSE_NODE_SHAPE],
                         data.shape)
        self.assertEqual(response[CMD_KW_DATA][RESPONSE_NODE_DTYPE],
                         data.dtype)

    def test_slice(self):
        db_name = 'test.h5'
        ds_name = 'testds'
        data = np.random.randint(0, 255, size=(5, 10)).astype('uint8')

        self.create_db(db_name)
        self.create_ds(db_name, ds_name, data)

        cmd = {
            CMD_KW_CMD: CMD_SLICE_DATASET,
            CMD_KW_ARGS: {
                CMD_KW_DB: db_name,
                CMD_KW_PATH: ds_name
            }
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        cmd[CMD_KW_ARGS][CMD_KW_KEY] = slice(0, 0, 0)  # invalid slice
        response = unpack(handle_request(cmd))

        self.assertEqual(response[CMD_KW_STATUS], VALUE_ERROR)

        cmd[CMD_KW_ARGS][CMD_KW_KEY] = slice(0, 1, 1)
        response = unpack(handle_request(cmd))

        self.assertEqual(response[CMD_KW_STATUS], OK)
        assert_array_equal(response[RESPONSE_DATA], data[:1])

    def test_broadcast(self):
        db_name = 'test.h5'
        ds_name = 'testds'
        data = np.array([[1, 2, 3], [4, 5, 6]])

        self.create_db(db_name)
        self.create_ds(db_name, ds_name, data)

        cmd = {
            CMD_KW_CMD: CMD_BROADCAST_DATASET,
            CMD_KW_ARGS: {
                CMD_KW_DB: db_name,
                CMD_KW_PATH: ds_name
            }
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_DATA)

        new_data = np.array([8, 9, 10])
        cmd[CMD_KW_DATA] = new_data

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        cmd[CMD_KW_ARGS][CMD_KW_KEY] = slice(0, 0, 0)  # invalid slice
        response = unpack(handle_request(cmd))

        self.assertEqual(response[CMD_KW_STATUS], VALUE_ERROR)

        cmd[CMD_KW_DATA] = np.array([8, 9, 10, 11])  # invalid data
        cmd[CMD_KW_ARGS][CMD_KW_KEY] = slice(0, 1, 1)
        response = unpack(handle_request(cmd))

        self.assertEqual(response[CMD_KW_STATUS], TYPE_ERROR)

        cmd[CMD_KW_DATA] = new_data
        response = unpack(handle_request(cmd))

        self.assertEqual(response[CMD_KW_STATUS], OK)

        cmd = {
            CMD_KW_CMD: CMD_SLICE_DATASET,
            CMD_KW_ARGS: {
                CMD_KW_DB: db_name,
                CMD_KW_PATH: ds_name,
                CMD_KW_KEY: slice(None, None, None)
            }
        }

        response = unpack(handle_request(cmd))
        assert_array_equal(response[RESPONSE_DATA], [new_data, data[1]])

    def test_attrs(self):
        db_name = 'test.h5'
        ds_name = 'testds'
        data = np.array([[1, 2, 3], [4, 5, 6]])

        self.create_db(db_name)
        self.create_ds(db_name, ds_name, data)

        cmd = {
            CMD_KW_CMD: CMD_ATTRIBUTES_SET,
            CMD_KW_ARGS: {
                CMD_KW_DB: db_name,
                CMD_KW_PATH: ds_name
            }
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        attr_key = 'key'

        cmd[CMD_KW_ARGS][CMD_KW_KEY] = attr_key
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_DATA)

        attr_data = 'test'
        cmd[CMD_KW_DATA] = attr_data
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], OK)

        cmd = {
            CMD_KW_CMD: CMD_ATTRIBUTES_GET,
            CMD_KW_ARGS: {
                CMD_KW_DB: db_name,
                CMD_KW_PATH: ds_name
            }
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], MISSING_ARGUMENT)

        cmd[CMD_KW_ARGS][CMD_KW_KEY] = 'invalid'
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], KEY_ERROR)

        cmd[CMD_KW_ARGS][CMD_KW_KEY] = attr_key
        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_STATUS], OK)
        self.assertEqual(response[CMD_KW_DATA][RESPONSE_DATA], attr_data)

        cmd[CMD_KW_CMD] = CMD_ATTRIBUTES_CONTAINS
        response = unpack(handle_request(cmd))

        self.assertEqual(response[CMD_KW_STATUS], OK)
        self.assertTrue(response[CMD_KW_DATA][RESPONSE_ATTRS_CONTAINS])

        cmd[CMD_KW_ARGS][CMD_KW_KEY] = 'invalid'
        response = unpack(handle_request(cmd))
        self.assertFalse(response[CMD_KW_DATA][RESPONSE_ATTRS_CONTAINS])

        cmd = {
            CMD_KW_CMD: CMD_ATTRIBUTES_KEYS,
            CMD_KW_ARGS: {
                CMD_KW_DB: db_name,
                CMD_KW_PATH: ds_name
            }
        }

        response = unpack(handle_request(cmd))
        self.assertEqual(response[CMD_KW_DATA][RESPONSE_ATTRS_KEYS],
                         (attr_key,))
