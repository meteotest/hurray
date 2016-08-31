"""
Cross-process readers/writer synchronization.

The algorithm implemented is "Problem 2" in the following paper:
http://cs.nyu.edu/~lerner/spring10/MCP-S10-Read04-ReadersWriters.pdf
The solution proposed there is based on threads. Here, we implement the
same algorithm for process-based concurrency and for any number of shared
resources (as opposed to just one), i.e., hdf5 files. This requires the
use of named semaphores instead of simple variables.

Note that semaphore names must begin with a slash, followed by,
typically, 251 characters, none of which are slashes.
Example: /my_resource
Cf. http://linux.die.net/man/7/sem_overview for details on Linux.
"""

import contextlib
from functools import wraps
from itertools import product

import posix_ipc
from posix_ipc import Semaphore, BusyError


# constants
DEFAULT_TIMEOUT = 20  # seconds
# maximum length for semaphore names
# TODO can this be set according to OS configuration?
SEM_NAME_MAX = 251

# semaphore names/patterns
MUTEX3 = '/hfive_mutex3__{}'
MUTEX1 = '/hfive_mutex1__{}'
MUTEX2 = '/hfive_mutex2__{}'
R = '/hfive_r__{}'  # read lock
W = '/hfive_w__{}'  # write lock
READCOUNT = '/hfive_readcount__{}'
WRITECOUNT = '/hfive_writecount__{}'


def reader(f):
    """
    Decorates class instance methods reading an HDF5 file. Class instances
    must provide an attribute self.dbname (must not contain slashes).
    """

    @wraps(f)
    def func_wrapper(self, *args, **kwargs):
        """
        Wraps reading functions.
        """
        # names of mutexes/locks
        res_name = self.dbname
        mutex3 = MUTEX3.format(res_name)
        mutex1 = MUTEX1.format(res_name)
        r = R.format(res_name)
        w = W.format(res_name)

        readcount = Semaphore(READCOUNT.format(res_name),
                              flags=posix_ipc.O_CREAT, initial_value=0)

        # Note that try/finally must cover incrementing readcount as well
        # as acquiring w. Otherwise readcount/w cannot be
        # decremented/released if the reading operation fails.
        readcount_val = None
        try:
            with mutex(mutex3):
                with mutex(r):
                    # mutex1's purpose is to make readcount++ together with
                    # the readcount == 1 check atomic
                    with mutex(mutex1):
                        readcount.release()  # increment
                        readcount_val = readcount.value
                        # first reader sets the w lock to block writers
                        if readcount_val == 1:
                            acquire_mutex(w)
            result = f(self, *args, **kwargs)  # critical section
            return result
        finally:
            # if readcount was incremented above, we have to decrement it.
            # Also, if we are the last reader, we have to release w to open
            # the gate for writers.
            if readcount_val is not None:
                # again, mutex1's purpose is to make readcount-- and the
                # subsequent check atomic.
                with mutex(mutex1):
                    readcount.acquire()  # decrement
                    if readcount.value == 0:
                        release_mutex(w)
                        # Note that it's possible that, even though
                        # readcount was > 0, w was not set. This can
                        # happen if – during execution of the code
                        # above – a process terminated after
                        # readcount++ but before acquiring w.

    return func_wrapper


def writer(f):
    """
    Decorates methods writing to an HDF5 file.
    """

    @wraps(f)
    def func_wrapper(self, *args, **kwargs):
        """
        Wraps writing functions.
        """
        # names of locks
        res_name = self.dbname
        mutex2 = MUTEX2.format(res_name)
        r = R.format(res_name)
        w = W.format(res_name)

        # note that writecount may be > 1 as it also counts the waiting writers
        wc_name = WRITECOUNT.format(res_name)
        print(wc_name, posix_ipc.O_CREAT)
        writecount = Semaphore(wc_name, flags=posix_ipc.O_CREAT,
                               initial_value=0)

        writecount_val = None
        try:
            # mutex2's purpose is to make writecount++ together with
            # the writecount == 1 check atomic
            with mutex(mutex2):
                writecount.release()  # increment
                writecount_val = writecount.value
                # first writer sets r to block readers
                if writecount.value == 1:
                    acquire_mutex(r)

            # execute critical section ,i.e., writing
            with mutex(w):
                return_val = f(self, *args, **kwargs)
                return return_val
        finally:
            # if writecount was incremented above, we have to decrement it.
            # Also, if we are the last writer, we have to release r to open
            # the gate for readers.
            if writecount_val is not None:
                with mutex(mutex2):
                    writecount.acquire()  # decrement
                    if writecount.value == 0:
                        release_mutex(r)
                        # Note that it's possible that, even though
                        # writecount was > 0, r was not set. This can
                        # happen if – during execution of the code
                        # above – a process terminated after
                        # writecount++ but before acquiring w.

    return func_wrapper


class LockException(Exception):
    """
    Raises when a lock could not be acquired
    """
    pass


@contextlib.contextmanager
def mutex(name, acq_timeout=DEFAULT_TIMEOUT):
    """
    Allows atomic execution of code blocks using 'with' syntax:

    with lock('mylock'):
        # critical section...

    Args:
        lockname: name of the lock
        acq_timeout: timeout for acquiring the lock in seconds.

    Raises:
        LockException if lock could not be acquired
    """
    acquire_mutex(name, acq_timeout)
    try:
        yield
    finally:
        release_mutex(name)


def acquire_mutex(name, timeout=DEFAULT_TIMEOUT):
    """
    Try to acquire mutex ``name`` for ``timeout`` seconds.

    Raises:
        LockException on failure
    """
    sem = Semaphore(name, flags=posix_ipc.O_CREAT, initial_value=1)
    try:
        sem.acquire(timeout)
    except BusyError:
        raise LockException("could not acquire mutex {}".format(name))
    else:
        assert(sem.value == 0)
        return True


def release_mutex(name):
    """
    Release named mutex
    """
    sem = Semaphore(name, flags=0)
    sem.release()
    assert(sem.value == 1)


def unlink_semaphore(name):
    """
    Delete a semaphore, ignore if it does not exist.
    """
    try:
        sem = Semaphore(name, flags=0)
    except posix_ipc.ExistentialError:
        pass
    else:
        sem.unlink()


def clear_semaphores(resource_names):
    """
    Unlink all semaphores (mutexes, counters, locks) for given resource names.

    Args:
        resource_names: list of resource names (strings)
    """
    sem_templates = [MUTEX1, MUTEX2, MUTEX3, R, W, READCOUNT, WRITECOUNT]

    print("Deleting {} semaphores..."
          .format(len(resource_names) * len(sem_templates)))

    for sem_template, name in product(sem_templates, resource_names):
        sem_name = sem_template.format(name)
        unlink_semaphore(sem_name)
