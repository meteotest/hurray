import unittest

import msgpack
import numpy as np
from hurray.msgpack_ext import encode_np_array, decode_np_array
from numpy.testing import assert_array_equal


class MsgPackTestCase(unittest.TestCase):
    def test_ndarray(self):
        data_in = np.random.randint(0, 255, dtype='u1', size=(5, 10))

        packed_nparray = msgpack.packb(data_in, default=encode_np_array, use_bin_type=True)
        unpacked_nparray = msgpack.unpackb(packed_nparray, object_hook=decode_np_array, encoding='utf-8')

        assert_array_equal(data_in, unpacked_nparray)

    def test_slice(self):
        slice_in = slice(0, 1, 0)

        packed_slice = msgpack.packb(slice_in, default=encode_np_array, use_bin_type=True)
        unpacked_slice = msgpack.unpackb(packed_slice, object_hook=decode_np_array, encoding='utf-8')

        self.assertEqual(slice_in, unpacked_slice)
