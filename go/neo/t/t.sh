#!/bin/bash -e
# run 1 mixed py/go NEO cluster

# port allocations
Abind=127.0.0.1:5551
Mbind=127.0.0.1:5552
Sbind=127.0.0.1:5553
Zbind=127.0.0.1:5554

# cluster name
cluster=pygotest

# logs
log=`pwd`/log
mkdir -p $log

# M{py,go}
# spawn master
Mpy() {
	# XXX --autostart=1 ?
	exec -a Mpy \
		neomaster --cluster=$cluster --bind=$Mbind --masters=$Mbind -r 1 -p 1 --logfile=$log/Mpy.log $@ &
}

Mgo() {
	exec -a Mgo \
		neo --log_dir=$log master -cluster=$cluster -bind=$Mbind
}

# Spy ...
# spawn storage
Spy() {
	# --adapter=...
	# --database=...
	# --engine=...
	exec -a Spy \
		neostorage --cluster=$cluster --bind=$Sbind --masters=$Mbind &
}

# Sgo <data.fs>
# spawn storage
Sgo() {
	exec -a Sgo \
		neo -log_dir=$log -alsologtostderr storage -cluster=$cluster -bind=$Sbind -masters=$Mbind $@ &
}


# Apy
# spawn admin
Apy() {
	exec -a Apy \
		neoadmin --cluster=$cluster --bind=$Abind --masters=$Mbind $@ &
}

# Zpy <data.fs>
# spawn zeo
Zpy() {
	exec -a Zpy \
		runzeo --address $Zbind --filename $@ 2>>$log/Zpy.log &
}


# ---- generate test data ----

var=`pwd`/var
mkdir -p $var

# generate data with many small (4K) objects
export WENDELIN_CORE_ZBLK_FMT=ZBlk1

work=128	# array size generated (MB)

# generate data in data.fs
fs1=$var/fs1
mkdir -p $fs1
genfs() {
	demo-zbigarray --worksize=$work gen $fs1/data.fs
	sync
}

#genfs
#time demo-zbigarray read $fs1/data.fs

#Zpy $fs1/data.fs
#sleep 1
#time demo-zbigarray read zeo://$Zbind

#Mpy --autostart=1
## sleep 0.2
#Sgo $fs1/data.fs

time demo-zbigarray read neo://$cluster@$Mbind

wait
exit


# --------

# spawn Mpy + Sgo
Mpy
sleep 0.2 # XXX temp for debug: so master could start listening and first S connect try is not error
Sgo ../../zodb/storage/fs1/testdata/1.fs
Apy

wait
