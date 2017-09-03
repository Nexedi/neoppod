#!/bin/bash -e
# run 1 mixed py/go NEO cluster

# port allocations
Abind=127.0.0.1:5551
Mbind=127.0.0.1:5552
Sbind=127.0.0.1:5553

# cluster name
cluster=pygotest

# logs
logdir=`pwd`/log
mkdir -p $logdir

# M{py,go}
# spawn master
Mpy() {
	# XXX --autostart=1 ?
	exec -a Mpy \
		neomaster --cluster=$cluster --bind=$Mbind --masters=$Mbind -r 1 -p 1 --logfile=$logdir/Mpy.log &
}

Mgo() {
	exec -a Mgo \
		neo --log_dir=$logdir master -cluster=$cluster -bind=$Mbind
}

# TODO Spy

# Sgo <data>
# spawn storage
Sgo() {
	exec -a Sgo \
		neo -log_dir=$logdir -alsologtostderr storage -cluster=$cluster -bind=$Sbind -masters=$Mbind $@ &
}



# spawn Mpy + Sgo
Mpy
sleep 0.2 # XXX temp for debug: so master could start listening and first S connect try is not error
Sgo ../../zodb/storage/fs1/testdata/1.fs

wait
