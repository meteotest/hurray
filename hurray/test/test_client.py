#!/usr/bin/env python

import time
from datetime import timedelta

from tornado.tcpclient import TCPClient

from tornado import gen, ioloop, queues
from tornado.options import options, define

concurrency = 10

define("host", default="localhost", help="TCP server host")
define("port", default=8888, help="TCP port to connect to")
define("message", default="ping", help="Message to send")


@gen.coroutine
def main():
    q = queues.Queue()
    start = time.time()
    fetching, fetched = set(), set()

    @gen.coroutine
    def fetch_url():
        current_url = yield q.get()
        stream = yield TCPClient().connect(options.host, options.port)
        print("Connected (#%s)" % current_url)
        yield stream.write(("%d\n" % current_url).encode())
        reply = yield stream.read_until(b"\n")
        print("Response from server (#%s)" % current_url)
        q.task_done()

    @gen.coroutine
    def worker():
        while True:
            yield fetch_url()

    for _ in range(10000):
        q.put(_)


    # Start workers, then wait for the work queue to be empty.
    for _ in range(concurrency):
        worker()

    yield q.join(timeout=timedelta(seconds=300))
        # assert fetching == fetched
        # print('Done in %d seconds, fetched %s URLs.' % (
        #    time.time() - start, len(fetched)))


if __name__ == '__main__':
    import logging

    logging.basicConfig()
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(main)
