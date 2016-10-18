#!/bin/sh -xe

N1M=`neoctl -a 127.0.0.1:5551 print node |grep MASTER |awk '{print $5}'`

NEO_ADMIN_PORT=5552     \
neosimple --adapter=SQLite --temp_dir=tmp/2 --logger --name neo2 --upstream_masters $N1M --upstream_cluster neo1 s1
