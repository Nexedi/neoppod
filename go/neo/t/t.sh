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
	# XXX --masters=?
	# XXX --autostart=1 ?
	neomaster --cluster=$cluster --bind=$Mbind -r 1 -p 1 --logfile=`pwd`/Mpy.log
}

Mgo() {
	neo master -cluster=$cluster -bind=$Mbind -log_dir=`pwd`
}

# TODO Spy

# Sgo <data>
# spawn storage
Sgo() {
	neo storage -cluster=$cluster -bind=$Sbind -masters=$Mbind -log_dir=`pwd` $@
}



# spawn Mpy + Sgo
Mpy &
