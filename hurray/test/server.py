import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Pipe

from hurray.ioloop import IOLoop
from hurray import gen
from hurray.iostream import StreamClosedError
from hurray.log import enable_quick_pretty_logging
from hurray.tcpserver import TCPServer

logger = logging.getLogger(__name__)


class EchoServer(TCPServer):
    @staticmethod
    def do_shit(conn):
        time.sleep(5)
        return "hey there"

    @gen.coroutine
    def handle_stream(self, stream, address):
        while True:
            try:
                data = yield stream.read_until(b"\n")
                conninfo = '' #data.decode("utf-8").replace('\n', '')
                print("Red data", data)

                parent_conn, child_conn = Pipe()

                pool = ProcessPoolExecutor(max_workers=1)
                fut = pool.submit(EchoServer.do_shit, child_conn)
                ret = yield fut
                pool.shutdown()

                # EchoServer.do_shit(1, 2, 3, blah='gaga')
                logger.info("Processing done (#%s)" % conninfo)

                if not data.endswith(b"\n"):
                    data = data + b"\n"
                yield stream.write(data)
            except StreamClosedError:
                logger.warning("Lost client at host %s", address[0])
                break
            except Exception as e:
                print(e)


def serve():
    enable_quick_pretty_logging()

    logger.info("Server %d", os.getpid())

    server = EchoServer()
    server.bind(8888)
    server.start(2)  # Forks multiple sub-processes
    logger.info("Listening on TCP port %d", 8888)
    IOLoop.current().start()


if __name__ == "__main__":
    # replaces 'from h5py import File'
    # import numpy as np
    # from h5pyswmr import File

    # f = File('test.h5', 'r')
    # create a dataset containing a 500x700 random array
    # f.create_dataset(name='/mygroup/mydataset', data=np.random.random((500, 700)))
    # read data back into memory
    # data = f['/mygroup/mydatasett'][:]
    # no need to explicitely close the file (files are opened/closed when accessed)
    # print(data)

    serve()
