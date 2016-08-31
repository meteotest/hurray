Notes
#####

Socket programming resources
============================

* https://docs.python.org/3.4/howto/sockets.html
* https://docs.python.org/3.4/library/socketserver.html
* http://software-engineer.gatsbylee.com/brief-summary-from-socket-to-simple-http-server-in-python/
* Foundations of Python Network Programming, 3rd edition, pages 42 and
  following, pages 115 and following

Array serialization for sending over the wire
=============================================

* http://matthewrocklin.com/blog/work/2016/04/14/dask-distributed-optimizing-protocol
* https://www.safaribooksonline.com/library/view/python-cookbook-3rd/9781449357337/ch11s13.html
* numpy msgpack example:
  https://github.com/lebedov/msgpack-numpy/blob/master/msgpack_numpy.py
* transmit arrays in hdf5 format?
  Cf. https://github.com/telegraphic/hickle
  +: compression, platform and language agnostic
  -: client also requires hdf5 Python- and C-library incl. dependencies.

Standard protocols:
* https://github.com/google/protobuf
* https://github.com/appnexus/pyrobuf
* http://msgpack.org/index.html
