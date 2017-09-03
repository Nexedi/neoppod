#!/bin/bash -e
# run 1 mixed py/go NEO cluster

# port allocations
Abind=127.0.0.1:5551
Mbind=127.0.0.1:5552
Sbind=127.0.0.1:5553

# cluster name
cluster=pygotest


# M{py,go}
# spawn master
Mpy() {
	# XXX --autostart=1 ?
	neomaster --cluster=$cluster --bind=$Mbind --masters=$Mbind -r 1 -p 1 --logfile=`pwd`/Mpy.log &
}

Mgo() {
	exec -a Mgo \
		neo --log_dir=`pwd` master -cluster=$cluster -bind=$Mbind
}

# TODO Spy

# Sgo <data>
# spawn storage
Sgo() {
	exec -a Sgo \
		neo -log_dir=`pwd` storage -cluster=$cluster -bind=$Sbind -masters=$Mbind $@ &
}



# spawn Mpy + Sgo
Mpy
Sgo ../../zodb/storage/fs1/testdata/1.fs

wait
