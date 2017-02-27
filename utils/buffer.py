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

import collections
import errno
import numbers
import socket

_ERRNO_WOULDBLOCK = (errno.EWOULDBLOCK, errno.EAGAIN)

if hasattr(errno, "WSAEWOULDBLOCK"):
    _ERRNO_WOULDBLOCK += (errno.WSAEWOULDBLOCK,)


def _merge_prefix(deque, size):
    """Replace the first entries in a deque of strings with a single
    string of up to size bytes.

    """
    if len(deque) == 1 and len(deque[0]) <= size:
        return
    prefix = []
    remaining = size
    while deque and remaining > 0:
        chunk = deque.popleft()
        if len(chunk) > remaining:
            deque.appendleft(chunk[remaining:])
            chunk = chunk[:remaining]
        prefix.append(chunk)
        remaining -= len(chunk)
    if prefix:
        deque.appendleft(type(prefix[0])().join(prefix))
    if not deque:
        deque.appendleft(b"")


def errno_from_exception(e):
    if hasattr(e, 'errno'):
        return e.errno
    elif e.args:
        return e.args[0]
    else:
        return None


class BufferFullError(Exception):
    """Exception raised by `Buffer` methods when the buffer is full.
    """


class Buffer(object):
    def __init__(self, socket, max_buffer_size=None,
                 read_chunk_size=None):

        self.max_buffer_size = max_buffer_size or 104857600
        # A chunk size that is too close to max_buffer_size can cause
        # spurious failures.
        self.read_chunk_size = min(read_chunk_size or 65536,
                                   self.max_buffer_size // 2)

        self._read_buffer = collections.deque()
        self._read_buffer_size = 0
        self._closed = False
        self.socket = socket

    def closed(self):
        """Returns true if the socket has been closed."""
        return self._closed

    def close(self):
        if not self.closed():
            self.socket.close()
            self.socket = None
            self._closed = True

    def read_from_socket(self):
        try:
            chunk = self.socket.recv(self.read_chunk_size)
        except socket.error as e:
            if e.args[0] in _ERRNO_WOULDBLOCK:
                return None
            else:
                raise
        if not chunk:
            self.close()
            return None
        return chunk

    def _consume(self, loc):
        if loc == 0:
            return b""
        _merge_prefix(self._read_buffer, loc)
        self._read_buffer_size -= loc
        return self._read_buffer.popleft()

    def read_bytes(self, num_bytes):
        """Read a number of bytes.
        """
        assert isinstance(num_bytes, numbers.Integral)
        # Read from socket if there is not enough data in the buffer
        if num_bytes > self._read_buffer_size:
            while not self.closed():
                while True:
                    try:
                        chunk = self.read_from_socket()
                    except (socket.error, IOError, OSError) as e:
                        if errno_from_exception(e) == errno.EINTR:
                            continue
                        self.close()
                        raise
                    break
                if chunk is None:
                    break
                self._read_buffer.append(chunk)
                self._read_buffer_size += len(chunk)
                if self._read_buffer_size > self.max_buffer_size:
                    self.close()
                    raise BufferFullError("Reached maximum read buffer size")

                if self._read_buffer_size >= num_bytes:
                    break

        return self._consume(num_bytes)

    def write(self, data):
        """Write the given data to this socket.
        """
        assert isinstance(data, bytes)

        if data and not self.closed():
            write_buffer_size = 0
            write_buffer = collections.deque()
            # Break up large contiguous strings before inserting them in the
            # write buffer, so we don't have to recopy the entire thing
            # as we slice off pieces to send to the socket.
            WRITE_BUFFER_CHUNK_SIZE = 128 * 1024
            for i in range(0, len(data), WRITE_BUFFER_CHUNK_SIZE):
                write_buffer.append(data[i:i + WRITE_BUFFER_CHUNK_SIZE])
            write_buffer_size += len(data)

            while write_buffer:
                try:
                    num_bytes = self.socket.send(write_buffer[0])
                    _merge_prefix(write_buffer, num_bytes)
                    write_buffer.popleft()
                    write_buffer_size -= num_bytes
                except (socket.error, IOError, OSError) as e:
                    self.close()
                    raise
