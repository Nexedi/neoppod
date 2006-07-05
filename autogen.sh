#! /bin/sh

set -x
aclocal
autoheader
automake -a -c
autoconf
