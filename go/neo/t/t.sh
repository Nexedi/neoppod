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

xneoctl() {
	neoctl -a $Abind $@
}

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
		neostorage --cluster=$cluster --bind=$Sbind --masters=$Mbind --logfile=$log/Spy.log $@ &
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

# spawn NEO/py cluster working on sqlite db
neolite=$var/neo.sqlite
neopylite() {
	Mpy --autostart=1
	Spy --adapter=SQLite --database=$neolite
	Apy
}

# spawn NEO/py cluster working on InnoDB
neosql=$var/neo.sql
mkdir -p $neosql

# setup/spawn mariadb
mycnf=$neosql/mariadb.cnf
mysock=$(realpath $neosql)/my.sock
MDB() {
	cat >$mycnf <<EOF
[mysqld]
skip_networking
socket          = $mysock
datadir         = $neosql/data

# the following comes from
# https://lab.nexedi.com/nexedi/slapos/blob/master/software/neoppod/my.cnf.in#L18
# ---- 8< ----

# kirr: disabled
#plugin-load = ha_tokudb;ha_rocksdb

log_warnings = 1
disable-log-bin

## The following settings come from ERP5 configuration.

max_allowed_packet = 128M
query_cache_size = 32M
innodb_locks_unsafe_for_binlog = 1

# Some dangerous settings you may want to uncomment temporarily
# if you only want performance or less disk access.
#innodb_flush_log_at_trx_commit = 0
#innodb_flush_method = nosync
#innodb_doublewrite = 0
#sync_frm = 0

# Extra parameters.
log_slow_verbosity = explain,query_plan
# kirr: rocksb disabled
# rocksdb_block_cache_size = 10G
# rocksdb_max_background_compactions = 3
long_query_time = 1
innodb_file_per_table = 1

# Force utf8 usage
collation_server = utf8_unicode_ci
character_set_server = utf8
skip_character_set_client_handshake

[client]
socket = $mysock
user = root
EOF

	# setup system tables on first run
	if ! test -e $neosql/data ; then
		mysql_install_db --defaults-file=$mycnf
	fi

	mysqld --defaults-file=$mycnf &
}

xmysql() {
	set -x
	mysql --defaults-file=$mycnf "$@"
	set +x
}

neopysql() {
	MDB
	sleep 1	# XXX fragile
	xmysql -e "CREATE DATABASE IF NOT EXISTS neo"

	Mpy --autostart=1
	Spy --adapter=MySQL --engine=InnoDB --database=root@neo$mysock
	Apy
}

# generate data in mariadb
gensql() {
	neopysql
	demo-zbigarray --worksize=$work gen neo://$cluster@$Mbind
	xneoctl set cluster stopping
	sleep 1	# XXX fragile
	xmysql -e "SHUTDOWN"
	wait	# XXX fragile
	sync
}


# generate data in data.sqlite
gensqlite() {
	neopylite
	demo-zbigarray --worksize=$work gen neo://$cluster@$Mbind
	xneoctl set cluster stopping
	wait	# XXX fragile - won't work if there are childs spawned outside
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

#gensqlite
gensql

exit


#neopylite
#time demo-zbigarray read neo://$cluster@$Mbind
#xneoctl set cluster stopping


wait
exit


# --------

# spawn Mpy + Sgo
Mpy
sleep 0.2 # XXX temp for debug: so master could start listening and first S connect try is not error
Sgo ../../zodb/storage/fs1/testdata/1.fs
Apy

wait
