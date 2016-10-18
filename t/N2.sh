#!/bin/sh -xe

NEO_ADMIN_PORT=5552     \
neosimple --adapter=SQLite --temp_dir=tmp/2 --logger --name neo2 s1
