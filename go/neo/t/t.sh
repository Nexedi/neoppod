#!/bin/bash -e
# run 1 mixed py/go NEO cluster

# Mpy <cluster> <bind>
# spawn py master
Mpy() {
	# XXX --masters=?
	neomaster --cluster=$1 --bind=$2 -r 1 -p 1 --autostart=1 --logfile=`pwd`/master.log
}


# Sgo <cluster> <bind> <masters> <data>
# spwan go storage
Sgo() {
	neo storage -cluster=$1 -bind=$2 -masters=$3 -logdir=`pwd` $4
}
