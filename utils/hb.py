#!/usr/bin/env python
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
import concurrent.futures
import random
import socket
import string
import struct
import time

import msgpack
import numpy as np

from buffer import Buffer
from proto import encode, decode, PROTOCOL_VER, CMD_KW_DB, CMD_KW_OVERWRITE, CMD_KW_CMD, CMD_KW_ARGS, CMD_KW_DATA, \
    MSG_LEN, \
    CMD_CREATE_DATABASE, CMD_KW_PATH, CMD_CREATE_DATASET, CMD_KW_KEY, CMD_SLICE_DATASET, CMD_KW_STATUS, \
    CMD_BROADCAST_DATASET

MAX_SHAPE_SIZE = 850
DS_PATH = '/myds'


def random_name(l):
    return ''.join(random.SystemRandom().choice(string.ascii_lowercase)
                   for _ in range(l))


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1000.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1000.0
    return "%.1f %s%s" % (num, 'Y', suffix)


def connect(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.connect((host, port))
    v_print(1, 'Connected to %s:%d' % (host, port))
    return Buffer(s)


def exec(buffer, cmd, args, data=None):
    msg = msgpack.packb({
        CMD_KW_CMD: cmd,
        CMD_KW_ARGS: args,
        CMD_KW_DATA: data
    }, default=encode, use_bin_type=True)

    payload = struct.pack('>I', PROTOCOL_VER)
    payload += struct.pack('>I', len(msg))
    payload += msg
    buffer.write(payload)
    buffer.read_bytes(MSG_LEN)
    msg_data = buffer.read_bytes(struct.unpack('>I', buffer.read_bytes(MSG_LEN))[0])
    resp = msgpack.unpackb(msg_data, object_hook=decode, use_list=False, encoding='utf-8')
    v_print(3, 'Response Status: %s' % resp[CMD_KW_STATUS])
    return resp


def create_file(buffer):
    file_name = 'htest-' + random_name(5) + '.h5'
    exec(buffer, CMD_CREATE_DATABASE, {
        CMD_KW_DB: file_name,
        CMD_KW_OVERWRITE: True,
    })
    v_print(3, 'Created file %s' % file_name)
    return file_name


def create_dataset(buffer, db, data):
    exec(buffer, CMD_CREATE_DATASET, {
        CMD_KW_PATH: DS_PATH,
        CMD_KW_DB: db
    }, data)
    v_print(3, "Created dataset at '%s'" % DS_PATH)


def slice_dataset(buffer, db, key):
    resp = exec(buffer, CMD_SLICE_DATASET, {
        CMD_KW_PATH: DS_PATH,
        CMD_KW_DB: db,
        CMD_KW_KEY: key
    })
    v_print(3, "Sliced dataset at '%s'" % DS_PATH)
    return resp


def broadcast_dataset(buffer, db, key, data):
    resp = exec(buffer, CMD_BROADCAST_DATASET, {
        CMD_KW_PATH: DS_PATH,
        CMD_KW_DB: db,
        CMD_KW_KEY: key
    }, data)
    v_print(3, "Broadcasted dataset at '%s'" % DS_PATH)
    return resp


def worker(host, port, requests, file_name=None):
    """
    Creates a new file, dataset ([MAX_SHAPE_SIZE, MAX_SHAPE_SIZE]) and send requests to hurray server: 10% write and 90% read with
    random ([4,4] - [MAX_SHAPE_SIZE, MAX_SHAPE_SIZE]) slice sizes.
    :param host:
    :param port:
    :param requests:
    :param file_name:
    :return:
    """
    buffer = connect(host, port)
    if not file_name:
        file_name = create_file(buffer)
        create_dataset(buffer, file_name, np.random.random((MAX_SHAPE_SIZE, MAX_SHAPE_SIZE)))
    stats = {"errors": 0, "time": [], "read": 0, "write": 0, 'data': 0}
    for request in range(requests):
        start = random.randint(0, MAX_SHAPE_SIZE)
        if start == MAX_SHAPE_SIZE:
            start -= 1
        end = random.randint(start + 1, MAX_SHAPE_SIZE)
        s = slice(start, end)
        if random.randint(0, 10) < 1:
            st = time.perf_counter()
            data = np.random.random((end - start, MAX_SHAPE_SIZE))
            stats["data"] += data.nbytes
            resp = broadcast_dataset(buffer, file_name, s, data)
            stats["time"] += [time.perf_counter() - st]
            if resp[CMD_KW_STATUS] >= 200:
                stats["errors"] += 1
            stats["write"] += 1
        else:
            st = time.perf_counter()
            resp = slice_dataset(buffer, file_name, s)
            stats["data"] += resp["data"].nbytes
            stats["time"] += [time.perf_counter() - st]
            if resp[CMD_KW_STATUS] >= 200:
                stats["errors"] += 1
            stats["read"] += 1
    buffer.close()
    return stats


def stress(host, port, requests, concurrency, multiple=False):
    print("Benchmarking %s:%d (be patient)" % (host, port), end='', flush=True)
    file_name = None
    if not multiple:
        buffer = connect(host, port)
        file_name = create_file(buffer)
        create_dataset(buffer, file_name, np.random.random((MAX_SHAPE_SIZE, MAX_SHAPE_SIZE)))
        buffer.close()
    summary = {"errors": 0, "time": [], "read": 0, "write": 0, "data": 0}
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        workers = [executor.submit(worker, host, port, requests, file_name) for w in range(concurrency)]
        for future in concurrent.futures.as_completed(workers):
            stats = future.result()
            for k in summary.keys():
                summary[k] += stats[k]
            print('.', end='', flush=True)
    print("done\n")

    request = summary["read"] + summary["write"]
    print("Concurrency Level:\t%d" % concurrency)
    print("Time taken for tests:\t%.2f seconds" % sum(summary["time"]))
    print("Complete requests:\t%d (%d read, %d write)" % (request, summary["read"], summary["write"]))
    print("Failed requests:\t%d" % summary["errors"])
    print("Total transferred:\t%s" % sizeof_fmt(summary["data"]))
    print("Time per request:\t%.2f [ms] (mean)" % (np.mean(summary["time"]) * 1000))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Hurray server benchmarking tool")
    parser.add_argument('host', metavar='hostname', type=str,
                        help='hostname')
    parser.add_argument('port', metavar='port', type=int,
                        help='port')
    parser.add_argument("-n", metavar='requests', type=int, default=1,
                        help="Number of requests to perform for the benchmarking session. "
                             "The default is to just perform a single request which usually "
                             "leads to non-representative bench‐marking results.")
    parser.add_argument("-c", metavar='concurrency', type=int, default=1,
                        help="Number of multiple requests to perform at a time. Default is one request at a time.")
    parser.add_argument("-m", metavar='multiple', type=bool, default=False,
                        help="Create and use an individual file for each concurrent worker")
    parser.add_argument("-v", metavar='verbosity', type=int, default=0,
                        help="How much troubleshooting info to print.")

    args = parser.parse_args()


    def v_print(v, *a, **k):
        if v <= args.v:
            print(*a, **k)


    stress(args.host, args.port, args.n, args.c)
