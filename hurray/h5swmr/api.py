"""
Wrapper around h5py that synchronizes reading and writing of hdf5 files
(parallel reading is possible, writing is serialized)

Access to hdf5 files is synchronized by a solution to the readers/writers
problem,
cf. http://en.wikipedia.org/wiki/Readers%E2%80%93writers_problem
#The_second_readers-writers_problem

!!! IMPORTANT !!!
Note that the locks used are not recursive/reentrant. Therefore, a synchronized
method (decorated by @reader or @writer) must *not* call other synchronized
methods, otherwise we get a deadlock!
"""

import os

import h5py

from .sync import reader, writer


class Node(object):
    """
    Wrapper for h5py.Node
    """

    def __init__(self, file, path):
        """
        Args:
            file: full path to hdf5 file
            path: full path to the hdf5 node (not to be confused with path of
                the file)
        """
        self.file = file
        self._path = path
        self.attrs = AttributeManager(self.file, self._path)

    @reader
    def __getitem__(self, key):
        """
        Raises:
            KeyError if object does not exist.
        """
        # sometimes the underlying hdf5 C library writes errors to stdout,
        # e.g., if a path is not found in a file.
        # cf. http://stackoverflow.com/questions/15117128/
        # h5py-in-memory-file-and-multiprocessing-error
        h5py._errors.silence_errors()

        if key.startswith('/'):  # absolute path
            path = key
        else:                    # relative path
            path = os.path.join(self.path, key)

        with h5py.File(self.file, 'r') as f:
            node = f[path]
            return self._wrap_class(node)

    @property
    def path(self):
        """
        wrapper
        """
        return self._path

    @property
    def name(self):
        """
        Name of the group or dataset
        """
        name = os.path.split(self._path)[1]
        # set root group's name to "/"
        name = "/" if name == "" else name

        return name

    def _wrap_class(self, node):
        """
        Wraps h5py objects into h5pyswmr objects.

        Args:
            node: instance of h5py.Group or h5py.Dataset

        Returns:
            Corresponding object as a h5pyswmr object

        Raises:
            TypeError if ``obj`` is of unknown type
        """
        if isinstance(node, h5py.File):
            return File(name=self.file, mode="r")
        if isinstance(node, h5py.Group):
            return Group(file=self.file, path=node.name)
        elif isinstance(node, h5py.Dataset):
            return Dataset(file=self.file, path=node.name)
        else:
            raise TypeError('unknown h5py node object')


class Group(Node):
    """
    Wrapper for h5py.Group
    """

    def __init__(self, file, path):
        Node.__init__(self, file, path)

    def __repr__(self):
        return "<HDF5 Group (path={0})>".format(self.path)

    @writer
    def create_group(self, name):
        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]
            created_group = group.create_group(name)
            path = created_group.name

        return Group(self.file, path=path)

    @writer
    def require_group(self, name):
        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]
            created_group = group.require_group(name)
            path = created_group.name

        return Group(self.file, path=path)

    @writer
    def create_dataset(self, **kwargs):
        overwrite = kwargs.get('overwrite', False)
        name = kwargs['name']
        # remove additional arguments because they are not supported by h5py
        try:
            del kwargs['overwrite']
        except Exception:
            pass
        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]
            if overwrite and name in group:
                del group[name]
            dst = group.create_dataset(**kwargs)
            path = dst.name

        return Dataset(self.file, path=path)

    @writer
    def require_dataset(self, **kwargs):
        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]
            dst = group.require_dataset(**kwargs)
            path = dst.name
        return Dataset(self.file, path=path)

    @reader
    def keys(self):
        with h5py.File(self.file, 'r') as f:
            # w/o list() it does not work with py3 (returns a view on a closed
            # hdf5 file)
            keys = list(f[self.path].keys())
            return keys

    # TODO visit() and visititems() do not yet work because @reader methods
    # are not reentrant! => just wrap code into an inner function!

    # @reader
    # def visit(self, func):
    #     """
    #     Wrapper around h5py.Group.vist()

    #     Args:
    #         func: a unary function
    #     """
    #     with h5py.File(self.file, 'r') as f:
    #         return f[self.path].visit(func)

    # @reader
    # def visititems(self, func):
    #     """
    #     Wrapper around h5py.Group.visititems()

    #     Args:
    #         func: a 2-ary function
    #     """
    #     with h5py.File(self.file, 'r') as f:
    #         grp = f[self.path]
    #         def proxy(name):
    #             obj = self._wrap_class(grp[name])
    #             return func(name, obj)
    #         return self.visit(proxy)

    @reader
    def tree(self):
        """
        Return tree data structure consisting of all groups and datasets.
        A tree node is defined recursively as a tuple:

            [Dataset/Group, [children]]

        Returns: list
        """

        def buildtree(treenode):
            h5py_obj, children = treenode
            assert(children == [])
            if isinstance(h5py_obj, h5py.Dataset):
                # wrap h5py dataset object
                treenode[0] = self._wrap_class(h5py_obj)
                return
            else:
                # wrap h5py group object
                treenode[0] = self._wrap_class(h5py_obj)
                for name, childobj in h5py_obj.items():
                    newnode = [childobj, []]
                    children.append(newnode)
                    buildtree(newnode)

        tree = None
        with h5py.File(self.file, 'r') as f:
            root = f[self.path]
            tree = [root, []]  # [h5py object, children]
            buildtree(tree)

        return tree

    @reader
    def items(self):
        """
        Returns a list of (name, object) pairs for objects directly
        attached to this group. Values for broken soft or external links
        show up as None.
        Note that this differs from h5py, where a list (Py2) or a
        "set-like object" (Py3) is returned.
        """
        result = []
        with h5py.File(self.file, 'r') as f:
            for name, obj in f[self.path].items():
                result.append((name, self._wrap_class(obj)))

        return result

    @reader
    def __contains__(self, key):
        with h5py.File(self.file, 'r') as f:
            group = f[self.path]
            return key in group

    @writer
    def __delitem__(self, key):
        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]
            del group[key]


class File(Group):
    """
    Wrapper for h5py.File
    """

    def __init__(self, name, mode="r", *args, **kwargs):
        """
        try to open/create an h5py.File object
        """

        # call h5py.File() in case file needs to be created
        if mode in ("w", "w-", "x", "a"):

            self.file = name  # this is crucial for the @writer annotation

            @writer
            def init(self):
                with h5py.File(name=name, mode=mode, *args, **kwargs):
                    pass

            init(self)

        Group.__init__(self, name, '/')

    def __enter__(self):
        """
        simple context manager (so we can use 'with File() as f')
        """
        # TODO can be removed
        return self

    def __exit__(self, type, value, tb):
        pass

    def __repr__(self):
        return "<HDF5 File ({0})>".format(self.file)

    @property
    @reader
    def filesize(self):
        """
        Return size of the file in bytes
        """
        stat = os.stat(self.file)
        return stat.st_size

class Dataset(Node):
    """
    Wrapper for h5py.Dataset
    """

    def __init__(self, file, path):
        Node.__init__(self, file, path)

    @reader
    def __getitem__(self, slice):
        """
        implement multidimensional slicing for datasets
        """
        with h5py.File(self.file, 'r') as f:
            return f[self.path][slice]

    @writer
    def __setitem__(self, slice, value):
        """
        Broadcasting for datasets. Example: mydataset[0,:] = np.arange(100)
        """
        with h5py.File(self.file, 'r+') as f:
            f[self.path][slice] = value

    @writer
    def resize(self, size, axis=None):
        with h5py.File(self.file, 'r+') as f:
            f[self.path].resize(size, axis)

    @property
    @reader
    def shape(self):
        with h5py.File(self.file, 'r') as f:
            return f[self.path].shape

    @property
    @reader
    def dtype(self):
        with h5py.File(self.file, 'r') as f:
            return f[self.path].dtype


class AttributeManager(object):
    """
    Provides same features as AttributeManager from h5py.
    """

    def __init__(self, h5file, path):
        """
        Args:
            h5file: file name of hdf5 file
            path: full path to hdf5 node
        """
        self.file = h5file
        self.path = path

    @reader
    def __iter__(self):
        # In order to be compatible with h5py, we return a generator.
        # However, to preserve thread-safety, we must make sure that the hdf5
        # file is closed while the generator is being traversed.
        with h5py.File(self.file, 'r') as f:
            node = f[self.path]
            keys = [key for key in node.attrs]

        return (key for key in keys)

    @reader
    def keys(self):
        """
        Returns attribute keys (list)
        """
        with h5py.File(self.file, 'r') as f:
            node = f[self.path]
            return list(node.attrs.keys())

    @reader
    def __contains__(self, key):
        with h5py.File(self.file, 'r') as f:
            node = f[self.path]
            return key in node.attrs

    @reader
    def __getitem__(self, key):
        with h5py.File(self.file, 'r') as f:
            node = f[self.path]
            return node.attrs[key]

    @writer
    def __setitem__(self, key, value):
        with h5py.File(self.file, 'r+') as f:
            node = f[self.path]
            node.attrs[key] = value

    @writer
    def __delitem__(self, key):
        with h5py.File(self.file, 'r+') as f:
            node = f[self.path]
            del node.attrs[key]

    @reader
    def get(self, key, defaultvalue):
        """
        Return attribute value or return a default value if key is missing.
        Args:
            key: attribute key
            defaultvalue: default value to be returned if key is missing
        """
        with h5py.File(self.file, 'r') as f:
            node = f[self.path]
            return node.get(key, defaultvalue)
