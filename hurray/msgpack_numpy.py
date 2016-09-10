import numpy as np
from numpy.lib.format import header_data_from_array_1_0


def encode_np_array(obj):
    """
    Data encoder for serializing numpy arrays.
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

    return obj


def decode_np_array(obj):
    """
    Decoder for deserializing numpy array.
    """

    if b'__ndarray__' in obj:
        arr = np.fromstring(obj[b'data'], dtype=np.dtype(obj[b'descr']))
        shape = obj[b'shape']
        arr.shape = shape
        if obj[b'fortran_order']:
            arr.shape = shape[::-1]
            arr = arr.transpose()
        return arr
    elif b'__slice__' in obj:
        return slice(*obj[b'__slice__'])

    return obj
