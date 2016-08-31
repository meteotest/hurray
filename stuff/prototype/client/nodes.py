"""
Hdf5 entities (Nodes, Groups, Datasets)
"""

import os

import numpy as np

from hfivepy.const import OK, GROUP_EXISTS, DATASET_EXISTS


class Node():
    """
    HDF5 node
    """

    def __init__(self, conn, path):
        """
        Args:
            conn: Connection object
            path: full path to the hdf5 node
        """
        self._conn = conn
        self._path = path
        # every node has an attrs property
        self.attrs = AttributeManager(self._conn, self._path)

    def _compose_path(self, name):
        """
        """
        if name.startswith('/'):  # absolute path
            return name
        else:  # relative path
            return os.path.join(self.path, name)

    def __getitem__(self, key):
        """
        Args:
            key: hdf5 path

        Returns:
            An instance of Node (or of a subclass).

        Raises:
            KeyError if object does not exist.
        """
        # note that class Dataset overrides this method

        path = self._compose_path(key)
        args = {
            'path': path,
        }
        result, _ = self._conn.send_rcv('get_node', args)

        if result['statuscode'] == OK:
            if result['nodetype'] == 'group':
                return Group(self._conn, path)
            elif result['nodetype'] == 'dataset':
                shape = tuple(result['shape'])  # compatibility with numpy
                dtype = result['dtype']
                return Dataset(self._conn, path, shape=shape, dtype=dtype)
            else:
                raise RuntimeError("server returned unknown node type")
        else:
            # TODO error handling
            raise KeyError("could not get item")

    @property
    def path(self):
        """
        wrapper
        """
        return self._path


class Group(Node):
    """
    HDF5 group
    """

    def __init__(self, conn, path):
        Node.__init__(self, conn, path)

    def __repr__(self):
        return "<HDF5 Group (db={}, path={})>".format(self._conn.db,
                                                      self._path)

    def create_group(self, name):
        """
        Args:
            name: name or path of the group, may contain slashes, e.g.,
                'group/subgroup'

        Raises:
            ValueError if group already exists
        """
        group_path = self._compose_path(name)
        args = {
            'path': group_path,
        }
        result, _ = self._conn.send_rcv('create_group', args)
        if result['statuscode'] == OK:
            return Group(self._conn, group_path)
        elif result['statuscode'] == GROUP_EXISTS:
            raise ValueError("Group already exists")

    def require_group(self, name):
        group_path = self._compose_path(name)
        args = {
            'path': group_path,
        }
        result, _ = self._conn.send_rcv('require_group', args)
        if result['statuscode'] == OK:
            return Group(self._conn, group_path)
        else:
            raise ValueError("TODO")

    def create_dataset(self, name, data=None, shape=None, init_value=0,
                       dtype=None, attrs=None):
        """
        Provide either ``data`` or both ``shape`` and ``init_value``.

        Args:
            name: name or path of the dataset
            data: numpy array
            shape: tuple denoting the shape of the array to be created
            init_value: initial value to be used to create array. Possible
                values: either a scaler (int, float) or 'random'
            dtype: if ``init_value`` is 'random', you can optionally provide
                a dtype.
            attrs: dictionary of attributes TODO

        Raises:
            ValueError is dataset already exists
        """
        dst_path = self._compose_path(name)
        if data is None:
            args = {
                'path': dst_path,
                'shape': shape,
                'init_value': init_value,
                'dtype': dtype
            }
        else:
            args = {
                'path': dst_path,
            }
        result, _ = self._conn.send_rcv('create_dataset', args, data)

        if result['statuscode'] == DATASET_EXISTS:
            raise ValueError("dataset already exists")
        else:
            return Dataset(self._conn, dst_path, shape=shape, dtype=dtype)

    def require_dataset(self, **kwargs):
        raise NotImplementedError()

    def keys(self):
        raise NotImplementedError()

    def items(self):
        """
        """
        raise NotImplementedError()

    def __contains__(self, key):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()


class Dataset(Node):
    """
    Wrapper for h5py.Dataset
    """

    def __init__(self, conn, path, shape, dtype):
        Node.__init__(self, conn, path)
        self.__shape = shape
        self.__dtype = dtype

    def __getitem__(self, key):
        """
        Multidimensional slicing for datasets

        Args:
            key: key object, e.g., slice() object

        Returns:
            Numpy array

        Raises:
            IndexError if ``key`` was illegal
        """
        # TODO check if dtype corresponds to self.dtype (dataset may have been
        # overwritten in the meantime)
        args = {
            'path': self.path,
            'key': key,  # will be json encoded
        }
        result, arr = self._conn.send_rcv('slice_dataset', args)

        if result['statuscode'] == OK:
            return arr
        else:
            # TODO error handling
            raise IndexError("could not get data")

    def __setitem__(self, key, value):
        """
        Broadcasting for datasets. Example: mydataset[0,:] = np.arange(100)
        """
        args = {
            'path': self.path,
            'key': key,  # will be json encoded
        }
        if isinstance(value, np.ndarray):
            arr = value
        else:
            arr = None
            args['value'] = value
        result, arr = self._conn.send_rcv('broadcast_dataset', args, arr)

        if result['statuscode'] == OK:
            return arr
        else:
            # TODO error handling
            raise ValueError("operation failed: {}".format(result['statuscode']))

    @property
    def shape(self):
        """
        Returns:
            a shape tuple
        """
        return self.__shape

    @property
    def dtype(self):
        """
        Returns:
            numpy dtype
        """
        return self.__dtype


class AttributeManager(object):
    """
    Provides same features as AttributeManager from h5py.
    """

    def __init__(self, conn, path):
        """
        Args:
            conn: Connection object
            path: full path to hdf5 node
        """
        self.__conn = conn
        self.__path = path

    def __iter__(self):
        raise NotImplementedError()

    def keys(self):
        """
        Returns attribute keys (list)
        """
        args = {
            'path': self.__path,
        }
        result, _ = self.__conn.send_rcv('attrs_keys', args)

        if result['statuscode'] == OK:
            return result['keys']
        else:
            # TODO error handling
            raise RuntimeError("Error")

    def __contains__(self, key):
        args = {
            'path': self.__path,
            'key': key,
        }
        result, arr = self.__conn.send_rcv('attrs_contains', args)
        if result['statuscode'] == OK:
            return result['contains']
        else:
            raise RuntimeError("Error")

    def __getitem__(self, key):
        """
        Get attribute value for given ``key``.

        Returns:
            a primitive object (string, number) of a numpy array.
        """
        args = {
            'path': self.__path,
            'key': key,
        }
        result, arr = self.__conn.send_rcv('attrs_getitem', args)
        if result['statuscode'] == OK:
            return arr if arr is not None else result['value']
        else:
            # TODO error handling
            raise RuntimeError("Error")

    def __setitem__(self, key, value):
        """
        Set/overwrite attribute ``key`` with given ``value`` (scalar, string,
        or numpy array).
        """
        args = {
            'path': self.__path,
            'key': key,
        }
        if isinstance(value, np.ndarray):
            arr = value
        else:
            arr = None
            args['value'] = value
        result, _ = self.__conn.send_rcv('attrs_setitem', args, arr)

        if result['statuscode'] == OK:
            pass
        else:
            # TODO error handling
            raise RuntimeError("Error")

    def __delitem__(self, key):
        raise NotImplementedError()

    def get(self, key, defaultvalue):
        """
        Return attribute value or return a default value if key is missing.

        Args:
            key: attribute key
            defaultvalue: default value to be returned if key is missing
        """
        args = {
            'path': self.__path,
            'key': key,
            'default': defaultvalue,
        }
        result, arr = self.__conn.send_rcv('attrs_get', args)
        if result['statuscode'] == OK:
            return arr if arr is not None else result['value']
        else:
            # TODO error handling
            print(result)
            raise RuntimeError("Error")

    def to_dict(self):
        """
        Return attributes as dict
        """
        raise NotImplementedError()
