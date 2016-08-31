"""
RPC layer
"""

import os

import numpy as np

from hfive.config import DATADIR
from hfive import hdf5

# status codes
# TODO move to separate file
OK = 0
FILE_EXISTS = 2
FILE_NOTFOUND = 3
DATASET_EXISTS = 4
NODE_NOTFOUND = 5
DATASET_BROADCAST_FAILED = 6
ILLEGAL_ATTRS_KEY = 7
GROUP_EXISTS = 8
NOT_IMPLEMENTED = 500


# TODO file names must not be longer than 251 characters because
# semaphore names are limited to that size!


def dispatch(cmd, args, arr=None):
    """
    Every method called here must return a pair
        {"statuscode": XY, ...more keys/values...}, array
    where array is either a numpy array or None.

    Args:
        cmd: command
        args: command arguments (dict)
        arr: numpy array or None

    Returns:
        (result, array), where result can be any object and array is either
        a numpy array or None.
    """
    db = args.get('db', None)
    path = args.get('path', None)

    if cmd == 'test':
        return test()
    elif cmd == 'create_db':
        name = args['name']
        return create_db(name)
    elif cmd == 'connect_db':
        name = args['name']
        return connect_db(name)
    elif cmd == 'create_group':
        return create_group(db, path)
    elif cmd == 'require_group':
        return require_group(db, path)
    elif cmd == 'create_dataset':
        shape = args.get('shape', None)
        init_value = args.get('init_value', None)
        dtype = args.get('dtype', None)
        return create_dataset(db, path, data=arr, shape=shape,
                              init_value=init_value, dtype=dtype)
    elif cmd == 'get_node':
        return get_node(db, path)
    elif cmd == 'slice_dataset':
        key = args['key']
        return slice_dataset(db, path, key)
    elif cmd == 'broadcast_dataset':
        key = args['key']
        # broadcast value can either be passed as a scalar, i.e., in args, or
        # as array data
        value = args.get('value', arr)
        return broadcast_dataset(db, path, key, value)
    # attribute manager
    elif cmd == "attrs_getitem":
        return attrs_getitem(db, path, args['key'])
    elif cmd == "attrs_get":
        return attrs_get(db, path, args['key'], args['default'])
    elif cmd == "attrs_setitem":
        # broadcast value can either be passed as a scalar, i.e., in args, or
        # as array data
        value = args.get('value', arr)
        return attrs_setitem(db, path, args['key'], value)
    elif cmd == "attrs_keys":
        return attrs_keys(db, path)
    elif cmd == "attrs_contains":
        key = args['key']
        return attrs_contains(db, path, key)
    else:
        return {"statuscode": NOT_IMPLEMENTED}, None


def test():
    """
    """
    # send some array
    array = np.array([[4.5, 4.5, 7.86], [3.34, 0.003, np.nan]])

    return {"statuscode": OK}, array


def create_db(db):
    """
    """
    filename = _get_db_filename(db)
    try:
        with hdf5.File(filename, 'w-'):
            pass
    except OSError:
        print("file exists")
        return {"statuscode": FILE_EXISTS}, None
    else:
        return {"statuscode": OK}, None


def connect_db(db):
    """
    """
    filename = _get_db_filename(db)
    try:
        with hdf5.File(filename, 'r'):
            pass
    except OSError:
        return {"statuscode": FILE_NOTFOUND}, None
    return {"statuscode": OK}, None


def get_node(db, path):
    """
    """
    try:
        node = _get_node(db, path)
    except KeyError:
        return {"statuscode": NODE_NOTFOUND}, None
    if isinstance(node, hdf5.Dataset):
        nodetype = 'dataset'
    else:
        nodetype = 'group'
    # attrs = node.attrs
    result = {
        "statuscode": OK,
        "nodetype": nodetype,
        # TODO "attrs": attrs.to_dict(),
    }
    if nodetype == 'dataset':
        result['shape'] = node.shape
        result['dtype'] = node.dtype.name
    return result, None


def create_group(db, path):
    """
    """
    root = _get_node(db, '/')
    try:
        root.create_group(path)
    except ValueError:
        return {"statuscode": GROUP_EXISTS}, None

    return {"statuscode": OK}, None


def require_group(db, path):
    """
    """
    root = _get_node(db, '/')
    root.require_group(path)
    return {"statuscode": OK}, None


def create_dataset(db, path, data=None, shape=None, init_value=0, dtype=None):
    """
    Args:
        db: database name
        path: full path of dataset
        data: numpy array or None
        shape: shape of dataset
        init_value: initial value of dataset
        dtype: data type of dataset

    Returns:
        tuple {"statuscode": CODE}, None
    """
    filename = _get_db_filename(db)
    if data is None:
        data = np.empty(shape, dtype)
        if init_value is not None:
            data[:] = init_value
    try:
        with hdf5.File(filename, 'a') as f:
            try:
                f.create_dataset(name=path, data=data)
            except RuntimeError as e:
                if 'already exists' in str(e):
                    return {"statuscode": DATASET_EXISTS}, None
    except OSError:
        return {"statuscode": FILE_NOTFOUND}, None
    else:
        return {"statuscode": OK}, None


def slice_dataset(db, path, key):
    """
    Args:
        db: db name
        path: dataset path
        key: slice object
    """
    # h5py coughs if key is a list
    if isinstance(key, list):
        key = tuple(key)
    try:
        node = _get_node(db, path)
    except KeyError:
        return {"statuscode": NODE_NOTFOUND}, None
    arr = node[key]
    return {"statuscode": OK}, arr


def broadcast_dataset(db, path, key, value):
    """
    Args:
        db: db name
        path: dataset path
        key: slice object
        value: scalar or numpy array
    """
    # h5py coughs if key is a list
    if isinstance(key, list):
        key = tuple(key)
    try:
        node = _get_node(db, path)
    except KeyError:
        return {"statuscode": NODE_NOTFOUND}, None
    try:
        node[key] = value
    except RuntimeError as e:
        # TODO log e
        print(e)
        return {"statuscode": DATASET_BROADCAST_FAILED}, None
    else:
        return {"statuscode": OK}, None


def _get_node(db, path):
    """
    Returns an hdf5.Node object

    Args:
        db: db name
        path: node path

    Raises:
        KeyError if path does not exist
        OSError if db does not exist
    """
    filename = _get_db_filename(db)
    f = hdf5.File(filename, 'r')
    return f[path]


def attrs_getitem(db, path, key):
    """
    Args:
        db: db name
        path: node path
        key: attrs key
    """
    try:
        node = _get_node(db, path)
    except KeyError:
        return {"statuscode": NODE_NOTFOUND}, None
    try:
        value = node.attrs[key]
        if isinstance(value, np.ndarray):
            return {"statuscode": OK}, value
        else:
            return {"statuscode": OK, "value": node.attrs[key]}, None
    except KeyError:
        return {"statuscode": ILLEGAL_ATTRS_KEY}, None


def attrs_get(db, path, key, default):
    """
    Args:
        db: db name
        path: node path
        key: attrs key
        default: default value
    """
    try:
        node = _get_node(db, path)
    except KeyError:
        return {"statuscode": NODE_NOTFOUND}, None
    try:
        value = node.attrs.get(key, default)
        if isinstance(value, np.ndarray):
            return {"statuscode": OK}, value
        else:
            return {"statuscode": OK, "value": value}, None
    except KeyError:
        return {"statuscode": ILLEGAL_ATTRS_KEY}, None


def attrs_contains(db, path, key):
    """
    Args:
        db: db name
        path: node path
        key: attrs key
    """
    try:
        node = _get_node(db, path)
    except KeyError:
        return {"statuscode": NODE_NOTFOUND}, None
    contains = key in node.attrs
    return {"statuscode": OK, "contains": contains}, None


def attrs_setitem(db, path, key, value):
    """
    Args:
        db: db name
        path: node path
        key: attrs key
        value: scalar of numpy array
    """
    try:
        node = _get_node(db, path)
    except KeyError:
        return {"statuscode": NODE_NOTFOUND}, None
    try:
        node.attrs[key] = value
    except KeyError:
        return {"statuscode": ILLEGAL_ATTRS_KEY}, None
    return {"statuscode": OK}, None


def attrs_keys(db, path):
    """
    Args:
        db: db name
        path: node path
    """
    try:
        node = _get_node(db, path)
    except KeyError:
        return {"statuscode": NODE_NOTFOUND}, None
    keys = node.attrs.keys()
    return {"statuscode": OK, "keys": keys}, None


def _get_db_filename(db):
    filename = os.path.join(DATADIR, db + '.h5')
    return filename
