FAQ
###


What is hurray?
***************

Hurray is a server (and a client) – written in Python – for storing
multidimensional arrays. It uses `hdf5 <http://h5py.org>`_ as a storage engine.


Does it support Python 2?
*************************

No. The hurray server and client only work with Python 3.4 and later.


Is hurray open source?
**********************

Yes it is. Both the hurray server and the client are
`BSD licensed <https://github.com/meteotest/hurray/blob/master/LICENSE.txt>`_.


What is the difference between hurray and ...?
**********************************************

...h5serv
=========

h5serv is a REST-based web service for hdf5 files. It is developed by the HDF
group and is optimized for feature completeness, but no so much for high I/O
performance (as of early 2017). It has some promising features, though. Check
the `project page <https://support.hdfgroup.org/projects/hdfserver/>`_ and the
`github page <https://github.com/HDFGroup/h5serv>`_.


...SciDB
========

`SciDB <http://www.paradigm4.com/>`_ is an array database with full `ACID
<https://en.wikipedia.org/wiki/ACID>`_ support. It is also a "computational
database" whose primary use case is "in database" analysis of very large
datasets. It has a Python client. However, SciDB is not the best choice for I/O
heavy applications as data retrieval is quite slow compared to hurray.


...THREDDS Data Server
======================

The `THREDDS Data Server
<http://www.unidata.ucar.edu/software/thredds/current/tds/TDS.html>`_  is a web
server – written in Java – that provides data access for scientific datasets
based on the OPeNDAP protocol. In contrast to hurray, data access is *read
only*. The purpose of THREDDS is to *share* data among scientists while hurray
is more of a data store / database.


Who is the company behind hurray?
*********************************

Hurray is developed by `Meteotest <https://www.meteotest.ch/>`_, a swiss
weather and software company.

.. image:: ./meteotest.png
    :width: 100px
