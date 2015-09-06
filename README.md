# OpenPACS

This project started as a modified DCMTK's dcmqrscp
program where the index file based database
was replaced to a PostgreSql one.

Later it was added the JPEG2000 compression and decompression
codec thanks to *fmjpeg2koj*.

How to build
------------

First build dcmtk, then openpacs:

1. cd dcmtk-3.6.1_20150629
2. cmake .
3. make
4. cd ..
5. cmake .
6. make


