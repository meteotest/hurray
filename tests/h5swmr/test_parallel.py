import unittest
import os
import multiprocessing
import tempfile
import time
import random

import numpy as np

from hurray.h5swmr import File


class TestHDF5(unittest.TestCase):

    def setUp(self):
        self.shape = (8000, 1500)
        tmpdir = tempfile.gettempdir()
        self.filename = os.path.join(tmpdir, 'paralleltest827348723.h5')

    def tearDown(self):
        os.unlink(self.filename)

    def test_parallel(self):
        """
        Test parallel read/write access
        """

        NO_WORKERS = 40
        f = File(self.filename, 'w')
        # create some datasets (to test reading)
        for i in range(NO_WORKERS):
            f.create_dataset(name='/testgrp/dataset{}'.format(i),
                             data=np.random.random(self.shape)
                             .astype(np.float32))

        def worker_read(i, hdf5file):
            """ reading worker """
            time.sleep(random.random())
            print("worker {0} is reading...".format(i))
            data = hdf5file['/testgrp/dataset{}'.format(i)][:]
            print("worker {0} is done reading.".format(i))
            self.assertEqual(data.shape, self.shape)

        def worker_write(i, hdf5file):
            """ writing worker """
            # do some reading
            # print(hdf5file.keys())
            # do some writing
            time.sleep(random.random())
            data = np.empty((4, self.shape[0], self.shape[1]), dtype=np.int32)
            data[:] = i*100
            # modify existing dataset
            dst = hdf5file['/testgrp/dataset{}'.format(i)]
            print("worker {0} is writing...".format(i))
            dst[0:50, ] = i
            print("worker {0} done writing.".format(i))

        jobs = []
        writers = []
        print("")
        for i in range(NO_WORKERS):
            if i % 4 == 0:
                p = multiprocessing.Process(target=worker_write, args=(i, f))
                writers.append(i)
            else:
                p = multiprocessing.Process(target=worker_read, args=(i, f))
            jobs.append(p)
            p.start()
            # p.join()

        # wait until all processes have terminated
        while True:
            time.sleep(0.3)
            all_terminated = not max((job.is_alive() for job in jobs))
            if all_terminated:
                break

        # then test if data was written correctly
        print("Testing if data was written correctly...")
        for i in writers:
            dst = f['/testgrp/dataset{}'.format(i)]
            self.assertTrue(np.all(dst[0:50, ] == i))


def run():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestHDF5)
    unittest.TextTestRunner(verbosity=2).run(suite)
