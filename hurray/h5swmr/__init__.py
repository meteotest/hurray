"""
A drop-in replacement for the h5py library. Allows "single write multiple
read" (SWMR) access to hdf5 files.
"""

from .api import File, Node, Dataset, Group

__all__ = ["File", "Node", "Dataset", "Group"]
