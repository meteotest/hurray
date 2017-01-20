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
Msgpack encoders and decoders for numpy "objects" (arrays, types,
scalars) and slices.
"""

from inspect import isclass

import numpy as np
from numpy.lib.format import header_data_from_array_1_0


def encode_np_array(obj):
    """
    Encode numpy arrays and slices
    :param obj: object to serialize
    :return: dictionary with encoded array or slice
    """
    if isinstance(obj, np.ndarray):
        arr = header_data_from_array_1_0(obj)
        arr['data'] = obj.tostring()
        arr['__ndarray__'] = True
        return arr
    elif isinstance(obj, slice):
        return {
            '__slice__': (obj.start, obj.stop, obj.step)
        }
    elif isclass(obj) and issubclass(obj, np.number):
        # make sure numpy type classes such as np.float64 (used, e.g., as dtype
        # arguments) are serialized to strings
        return obj().dtype.name
    elif isinstance(obj, np.dtype):
        return obj.name
    elif isinstance(obj, np.number):
        # convert to Python scalar
        return np.asscalar(obj)

    return obj


def decode_np_array(obj):
    """
    Decode numpy arrays and slices
    :param obj: object to decode
    :return: numpy array or slice
    """

    if '__ndarray__' in obj:
        arr = np.fromstring(obj['data'], dtype=np.dtype(obj['descr']))
        shape = obj['shape']
        arr.shape = shape
        if obj['fortran_order']:
            arr.shape = shape[::-1]
            arr = arr.transpose()
        return arr
    elif '__slice__' in obj:
        return slice(*obj['__slice__'])

    return obj
