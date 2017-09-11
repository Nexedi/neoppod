#!/bin/bash -e
# run tests and benchmarks against FileStorage, ZEO and various NEO/py{sql,sqlite}, NEO/go clusters
# XXX neo/go - must be `go install'ed`
# XXX neo/py, wendelin.core, ... - must be pip install'ed

# port allocations
Abind=127.0.0.1:5551
Mbind=127.0.0.1:5552
Sbind=127.0.0.1:5553
Zbind=127.0.0.1:5554

# disk allocation
log=`pwd`/log;		mkdir -p $log
var=`pwd`/var;		mkdir -p $var
fs1=$var/fs1;		mkdir -p $fs1		# FileStorage (and so ZEO and NEO/go) data
neolite=$var/neo.sqlite				# NEO/py: sqlite
neosql=$var/neo.sql;	mkdir -p $neosql	# NEO/py: mariadb
mycnf=$neosql/mariadb.cnf			# NEO/py: mariadb config
mysock=$(realpath $neosql)/my.sock		# NEO/py: mariadb socket

# cluster name
cluster=pygotest

# control started NEO cluster
xneoctl() {
	neoctl -a $Abind "$@"
}

# control started MariaDB
xmysql() {
	mysql --defaults-file=$mycnf "$@"
}

# if we are abnormally terminating
trap 'set +e
echo "E: abnormal termination - stopping..."
xneoctl set cluster stopping
sleep 1
xmysql -e "SHUTDOWN"
sleep 1
j="$(jobs -p)"
test -z "$j" && exit
echo "E: killing left jobs..."
jobs -l
kill $j' EXIT

# ---- start NEO nodes ----

# M{py,go} ...	- spawn master
Mpy() {
	# XXX run via relative path to neomaster? (../../neo/neomaster) so we do not need to `pip install -e` ?

	# XXX --autostart=1 ?
	exec -a Mpy \
		neomaster --cluster=$cluster --bind=$Mbind --masters=$Mbind -r 1 -p 1 --logfile=$log/Mpy.log "$@" &
}

Mgo() {
	exec -a Mgo \
		neo --log_dir=$log master -cluster=$cluster -bind=$Mbind
}

# Spy ...	- spawn NEO/py storage
Spy() {
	# --adapter=...
	# --database=...
	# --engine=...
	exec -a Spy \
		neostorage --cluster=$cluster --bind=$Sbind --masters=$Mbind --logfile=$log/Spy.log "$@" &
}

# Sgo <data.fs>	- spawn NEO/go storage
Sgo() {
	# XXX use `go run ...` so it does not need go install?

	# -alsologtostderr
	exec -a Sgo \
		neo -cpuprofile cpu.out -log_dir=$log storage -cluster=$cluster -bind=$Sbind -masters=$Mbind "$@" &
}


# Apy ...	- spawn NEO/py admin
Apy() {
	exec -a Apy \
		neoadmin --cluster=$cluster --bind=$Abind --masters=$Mbind --logfile=$log/Apy.log "$@" &
}

# Zpy <data.fs> ...	- spawn ZEO
Zpy() {
	exec -a Zpy \
		runzeo --address $Zbind --filename "$@" 2>>$log/Zpy.log &
}


# ---- start NEO clusters ----

# spawn NEO/go cluster (Sgo+Mpy+Apy) working on data.fs
NEOgo() {
	Mpy --autostart=1
	Sgo $fs1/data.fs
	Apy
}

# spawn NEO/py cluster working on sqlite db
NEOpylite() {
	Mpy --autostart=1
	Spy --adapter=SQLite --database=$neolite
	Apy
}

# spawn neo/py cluster working on mariadb
NEOpysql() {
	MDB
	sleep 1	# XXX fragile
	xmysql -e "CREATE DATABASE IF NOT EXISTS neo"

	Mpy --autostart=1
	Spy --adapter=MySQL --engine=InnoDB --database=root@neo$mysock
	Apy
}


# setup/spawn mariadb
MDB() {
	cat >$mycnf <<EOF
[mysqld]
skip_networking
socket		= $mysock
datadir		= $neosql/data
log_error	= $log/mdb.log

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
		# XXX --cross-bootstrap only to avoid final large print notice
		# XXX but cross-bootstrap filters out current host name from installed tables - is it ok?
		mysql_install_db --defaults-file=$mycnf --cross-bootstrap
	fi

	mysqld --defaults-file=$mycnf &
}

# ---- generate test data ----

# generate data with many small (4K) objects
export WENDELIN_CORE_ZBLK_FMT=ZBlk1

# XXX 32 temp - raise
#work=32	# array size generated (MB)
work=64
#work=512	# array size generated (MB)

# generate data in data.fs
GENfs() {
	test -e $var/generated.fs && return
	echo -e '\n*** generating fs1 data...'
	demo-zbigarray --worksize=$work gen $fs1/data.fs
	sync
	touch $var/generated.fs
}

# generate data in sqlite
GENsqlite() {
	test -e $var/generated.sqlite && return
	echo -e '\n*** generating sqlite data...'
	NEOpylite
	demo-zbigarray --worksize=$work gen neo://$cluster@$Mbind
	xneoctl set cluster stopping
	wait	# XXX fragile - won't work if there are childs spawned outside
	sync
	touch $var/generated.sqlite
}

# generate data in mariadb
GENsql() {
	test -e $var/generated.sql && return
	echo -e '\n*** generating sql data...'
	NEOpysql
	demo-zbigarray --worksize=$work gen neo://$cluster@$Mbind
	xneoctl set cluster stopping
	sleep 1	# XXX fragile
	xmysql -e "SHUTDOWN"
	wait	# XXX fragile
	sync
	touch $var/generated.sql
}


# ---- main driver ----

# data/versions
echo -n "# "; date --rfc-2822
echo -n "# "; grep "^model name" /proc/cpuinfo |head -1 |sed -e 's/model name\s*: //'
echo -n "# "; uname -a
echo -n "# "; python --version
echo -n "# "; go version
echo -n "# "; mysqld --version

# pyver <egg> (<showas>) - print version of egg
pyver() {
	return	# XXX temp to save time
	local egg=$1
	local showas=$2
	test "$showas" == "" && showas=$egg
	local loc
	local pyver
	{
		read loc
		read pyver
	} < <(python -c "import pkg_resources as p; e=p.require(\"$egg\")[0]; print(\"%s\n%s\" % (e.location, e.version))")
	local gitver=$(git -C $loc describe --long --dirty 2>/dev/null)
	local ver
	test "$gitver" != "" && ver="$gitver" || ver="$pyver"
	printf "# %-16s: %s\n" "$showas" "$ver"
}
pyver neoppod neo
pyver zodb
pyver zeo
pyver mysqlclient
pyver wendelin.core

# generate test databases
GENfs
GENsqlite
GENsql
wait
sync


# run benchmarks
N=`seq 2`	# XXX repeat benchmarks N time

# time1 <url>	- run benchmarks on the URL once
bench1() {
	url=$1
#	time demo-zbigarray read $url
#	./zsha1.py $url
	if [[ $url == zeo://* ]]; then
		echo "(skipping zsha1.go on ZEO -- Cgo does not support zeo:// protocol)"
		return
	fi
	go run zsha1.go --log_dir=$log $url
	go run zsha1.go --log_dir=$log -useprefetch $url
}

echo -e "\n*** FileStorage"
for i in $N; do
	bench1 $fs1/data.fs
done

# echo -e "\n*** ZEO"
# Zpy $fs1/data.fs
# for i in $N; do
# 	bench1 zeo://$Zbind
# done
# killall runzeo
# wait

# echo -e "\n*** NEO/py sqlite"
# NEOpylite
# for i in $N; do
# 	bench1 neo://$cluster@$Mbind
# done
# xneoctl set cluster stopping
# wait

# echo -e "\n*** NEO/py sql"
# NEOpysql
# for i in $N; do
# 	bench1 neo://$cluster@$Mbind
# done
# xneoctl set cluster stopping
# xmysql -e "SHUTDOWN"
# wait

echo -e "\n*** NEO/go"
NEOgo
for i in $N; do
	bench1 neo://$cluster@$Mbind
done
xneoctl set cluster stopping
wait

# all ok
trap - EXIT
exit
