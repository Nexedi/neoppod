#!/bin/sh -xe

NEO_ADMIN_PORT=5551     \
neosimple --adapter=SQLite --temp_dir=tmp/1 --logger --name neo1 s1
