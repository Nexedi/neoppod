Change History
==============

1.9 (2018-03-13)
----------------

A lot of performance improvements have been done on storage nodes for this
release, and some of them required changes in the storage format. In short,
the migration is done automatically, but you may want to read UPGRADE notes
for more details.

Performance:

- Speed up replication by sending bigger network packets,
  and by not getting object next_serial for nothing.
- Speed up reads by indexing 'obj' primarily by 'oid' (instead of 'tid').
- Optimize storage layout of raw data for replication.

Other storage changes:

- Disable data deduplication by default. --dedup option added.
- importer: do not crash if a backup cluster tries to replicate.
- importer: preserve 'packed' flag.

Master:

- Fix possible failure when reading data in a backup cluster with replicas.
- Fix generation of TID.
- Fix resumption of backup replication (internal or not).

Client:

- New 'cache-size' Storage option.
- Cache hit/miss statistics.
- Fix accounting of cache size.
- Preserve 'packed' flag on iteration.
- At startup, or after nodes are back, full load balancing could be prevented
  until some data are written.

Other:

- neolog: --from option now also tries to parse with `dateutil`_.
- neolog: add support for xz-compressed logs, using external xzcat commands.

.. _dateutil: https://dateutil.readthedocs.io/

1.8.1 (2017-11-07)
------------------

- Add support for OpenSSL >= 1.1.
- storage: fix possible crash when delaying replication requests.
- mysql: fix performance issues on read operations by using more index hints.

1.8 (2017-07-04)
----------------

This release mainly stabilizes NEO when it is used with several storage nodes,
fixing many race conditions involving events like transactional operations
(read/write, conflict resolution...), replication, partition table tweaking,
and all kinds of failures (node crashes, network cuts...). This includes a
rework of conflict resolution, to implement the long-awaited deadlock avoidance
(it was a limitation caused by object-level locking).

Similarly, having spare master nodes is not an experimental feature anymore:
the `election` (of the primary master) has been reimplemented, and it now
happens during the RECOVERING phase. This comes with a change about node
states: BROKEN/HIDDEN/UNKNOWN are removed, DOWN is renamed into UNKNOWN,
and TEMPORARILY_DOWN into DOWN.

And still for more resiliency, the new algorithm to tweak the partition table
is better at minimizing the amount of replication, and it does not discard
readable cells too quickly anymore: a partition can now have multiple FEEDING
cells, to avoid going below the wanted level of replication.

Other changes:

- General:

  - Packet timeouts have been removed.
    TCP keepalives are used instead of applicative pings.
  - Connection handshake between nodes is reviewed to make sure that they
    speak the same protocol before doing anything else, and report clearer
    error messages otherwise. A dangerous bug was that there was no protocol
    version check between neoctl and the admin node.
  - Proper handling of incoming packets for closed/aborted connections.
  - An exception while processing an answer could leave the handler switcher
    in the bad state.
  - In STOPPING cluster state, really wait for all transactions to be finished.
  - Several issues when undoing transactions with conflict resolutions
    have been fixed.
  - Delayed connection acceptation when the storage node is ready.

- Client:

  - Added support for `zodburi`_.
  - Fix load error during conflict resolution in case of late invalidation.
  - Do not wait tpc_vote to start resolving conflicts.
  - Fix harmless 'unexpected ... AnswerRequestIdentification' exceptions.

- Storage:

  - New --disable-drop-partitions option, which is useful for big databases
    because the current code to delete data of discarded cells is inefficient
    (this option should disappear in the future).
  - Prevent 2 nodes from working with the same database.
  - Discard answers from aborted replications.
    In some cases, this led to data corruption or crashes.

- MySQL backend:

  - Added support for RocksDB.
  - Do not flood logs when retrying to connect non-stop.
  - Do not retry a failing query forever.
  - By default, do not retry to connect to the server automatically.

- Tools:

  - neolog: new --decompress option.
  - neolog: new option to hide the node column.
  - neoctl: make the identification of the primary master easier with
    'print node'.

- A lot of improvements for developers and debugging.

.. _zodburi: https://docs.pylonsproject.org/projects/zodburi

1.7.1 (2017-01-18)
------------------

- Replication:

  - Fixed possibly wrong knowledge of cells' backup_tid when resuming backup.
    In such case, 'neoctl print ids' gave false impression that the backup
    cluster was up-to-date. This also resulted in an inconsistent database
    when leaving backup mode before that the issue resolved by itself.
  - Storage nodes now select the partition which is furthest behind. Previous
    criterion was such that in case of high upstream activity, the backup could
    even be stuck looping on a subset of partitions.
  - Fixed replication of unfinished imported transactions.

- Fixed abort before vote, to free the storage space used by the transaction.
  A new 'prune_orphan' neoctl command was added to delete unreferenced raw data
  in the database.

- Removed short storage option -R to reset the db.
  Help is reworded to clarify that --reset exits once done.

- The application receiving buffer size has been increased.
  This speeds up transfer of big packets.

- The master raised AttributeError at exit during recovery.

- At startup, the importer storage backend connected twice to the destination
  database.

1.7.0 (2016-12-19)
------------------

- Identification issues, mainly caused by id conflicts, are fixed:

  - Storage nodes now only accept clients that are known by the master.
  - When reconnecting to a master, a client get a new id if the previous id is
    already reallocated to another client.
  - The consequences were either crashes or clients being unable to connect.

- Added support for the latest versions of ZODB (4.4.4 & 5.0.1). A notable
  change is that lastTransaction() does not ping the master anymore (but it
  still causes a connection to the master if the client is disconnected).

- A cluster in BACKUPING state can now serve regular clients in read-only mode.
  But without invalidation yet, so clients must reconnect whenever they want
  to see newer data.

- Fixed crash of client nodes (including backup master) while trying to process
  notifications before complete initialization, instead of ignoring them.

- Client:

  - Fix race condition leading to invalid mapping between internal connection
    objects and their file descriptors. This resulted in KeyError exceptions.
  - Fix item eviction from cache, which could break loading from storage.
  - Better exception handling in tpc_abort.
  - Do not limit the number of open connections to storage nodes.

- Storage:

  - Fix crash when a client loses connection to the master just before voting.
  - MySQL: Force index for a few queries. Unfortunately, this is not perfect
    because sometimes MySQL still ignores our hints.
  - MySQL: Do not use unsafe TRUNCATE statement.

- Make 'neoctl print ids' display time of TIDs.
- Various neoctl/neolog formatting improvements/fixes.
- Plus a few other changes for debugging and developers, as well as small
  optimizations.

1.6.3 (2016-06-15)
------------------

- Added support for ZODB 4.x

- Clients are now able to recover from failures during tpc_finish when the
  transaction got successfully committed.

- Other fixes related to node disconnection:

  - storage: fix crash when a client disconnects just after it requested to
    finish a transaction
  - storage: fix crash when trying to replicate from an unreachable node
  - master: do never abort a prepared transaction (for example,
    a client disconnecting during tpc_finish could cause a crash)
  - client: fix invalidation issues when reconnecting to the master

- Client:

  - fix abort for storages where only current serials were checked
  - fix the count of history items in the cache

- neoctl: better error message when connection to admin fails

1.6.2 (2016-03-09)
------------------

- storage: switch to a maintained fork of MySQL-python (mysqlclient)
- storage: for better performance, the backend commit after an unlocked
  transaction is deferred by 1 second, with the hope it's merged by a
  subsequent commit (in case of a crash, the transaction is unlocked again),
  so there are only 2 commits per transaction during high activity
- client: optimize cache by not keeping items with counter=0 in history queue
- client: fix possible assertion failure on load in case of a late invalidation

1.6.1 (2016-01-25)
------------------

NEO repository has moved to https://lab.nexedi.com/nexedi/neoppod.git

- client: fix spurious connection timeouts
- client: add cache stats to information dumped on SIGRTMIN+2
- storage: when using the Importer backend, allow truncation after the last
  tid to import, during or after the import
- neoctl: don't print 'None' on successful check/truncate commands
- neolog: fix crash on unknown packets
- plus a few other changes for debugging and developers

1.6 (2015-12-02)
----------------

This release has changes in storage format. The upgrade is done automatically,
but only if the cluster was stopped cleanly: see UPGRADE notes for more
information.

- NEO did not ensure that all data and metadata were written on disk before
  tpc_finish, and it was for example vulnerable to ENOSPC errors. In order to
  minimize the risk of failures during tpc_finish, the writing of metadata to
  temporary tables is now done in tpc_vote. See commit `7eb7cf1`_ for more
  information about possible changes on performance side.

  This change comes with a new algorithm to verify unfinished data, which also
  fixes a bug discarding transactions with objects for which readCurrent was
  called.

- The RECOVERING/VERIFYING phases, as well as transitions from/to other states,
  have been completely reviewed, to fix many bugs:

  - Possible corruption of partition table.
  - The cluster could be stuck in RECOVERING or VERIFYING state.
  - The probability to have cells out-of-date when restarting several storage
    nodes simultaneously has been reduced.
  - During recovery, a newly elected master now always waits all the storage
    nodes with readable cells to be pending, in order to avoid a split of the
    database.
  - The last tid/oid could be wrong in several cases, for example after
    transactions are recovered during VERIFYING phase.

- neoctl gets a new command to truncate the database at an arbitrary TID.
  Internally, NEO was already able to truncate the database, because this was
  necessary to make the database consistent when leaving the backup mode.
  However, there were several bugs that caused the database to be partially
  truncated:

  - The master now first stores persistently the decision to truncate,
    so that it can recover from any kind of connection failure.
  - The cluster goes back to RUNNING state only after an acknowledgment from
    all storage nodes (including those without any readable cell) that they
    truncated.

- Storage:

  - As a workaround to fix holes if replication is interrupted after new data
    is committed, outdated cells always restart to replicate from the beginning.
  - The deletion of partial transactions during verification didn't try to free
    the associated raw data.
  - The MySQL backend didn't drop the 'bigdata' table when erasing the database.

- Handshaking SSL connections could be stuck when they're aborted.

- 'neoctl print ids' displays a new value in backup mode: the highest common TID
  up to which all readable cells have replicated, i.e. the TID at which the
  database would be truncated when leaving the backup mode.

.. _7eb7cf1: https://lab.nexedi.com/nexedi/neoppod/commit/7eb7cf1

1.5.1 (2015-10-26)
------------------

Several bugs and performance issues have been fixed in this release, mainly
in the storage node.

- Importer storage backend:

  - Fix retrieval of an object from ZODB when next serial in NEO.
  - Fix crash of storage nodes when a transaction is aborted.
  - Faster resumption when many transactions
    have already been imported to MySQL.

- MySQL storage backend:

  - Refuse to start if max_allowed_packet is too small.
  - Faster commit of transaction metadata.

- Replication & checking of replicas:

  - Fix crash when a corruption is found while checking TIDs.
    2 other issues remain unfixed: see BUGS.rst file.
  - Speed up checking of replicas, at the cost of storage nodes being
    less responsive to other events.

- The master wrongly sent invalidations for objects on which only readCurrent
  was called, which caused invalid entries in client caches, or assertion
  failures in Connection._setstate_noncurrent.

1.5 (2015-10-05)
----------------

In this version, the connectivity between nodes has been greatly improved:

- Added SSL support.
- IPv4 & IPv6 can be mixed: some nodes can have an IPv4 binding address,
  whereas other listen on IPv6.
- Version 1.4 fixed several cases where nodes could reconnect too quickly,
  using 100% CPU and flooding logs. This is now fixed completely, for example
  when a backup storage node was rejected because the upstream cluster was not
  ready.
- Tickless poll loop, for lower latency and CPU usage: nodes don't wake up
  every second anymore to check if a timeout has expired.
- Connections could be wrongly processed before being polled (for reading or
  writing). This happened if a file descriptor number was reallocated by the
  kernel for a connection, just after a connection was closed.

Other changes are:

- IStorage: history() did not wait the oid to be unlocked. This means that the
  latest version of an object could be missing from the result.
- Log files can now be specified in configuration files.
- ~(user) construction are expanded for all paths in configuration (file or
  command line). This does not concern non-daemon executables like neoctl.
- For neoctl, -l option now logs everything on disk automatically.
- The admin node do not reset anymore the list of known masters from
  configuration when reconnecting, for consistency with client nodes.
- Code refactoring and improvements to logging and debugging.
- An notable change in the test suite is that the occasional deadlocks that
  affected threaded tests have been fixed.

1.4 (2015-07-13)
----------------

This version comes with a change in the SQL tables format, to fix a potential
crash of storage nodes when storing values that only differ by the compression
flag. See UPGRADE notes if you think your application may be affected by this
bug.

- Performance and features:

  - 'Importer' storage backend has been significantly sped up.

  - Support for TokuDB has been added to MySQL storage backend. The engine is
    still InnoDB by default, and it can be selected via a new 'neostorage'
    option.

  - A 'neomaster' option has been added to automatically start a new cluster
    if the number of pending storage nodes is greater than or equal to the
    specified value.

- Bugfixes:

  - Storage crashed when reading empty transactions. We still need to decide
    whether NEO should:

    - continue to store such transactions;
    - ignore them on commit, like other ZODB implementation;
    - or fail on commit.

  - Storage crashed when a client tries to "steal" the UUID of another client.

  - Client could get stuck forever on unreadable cells when not connected to the
    master.

  - Client could only instantiate NEOStorage from the main thread, and the
    RTMIN+2 signal displayed logs for only 1 NEOStorage. Now, RTMIN+2 & RTMIN+3
    are setup when neo.client module is imported.

- Plus fixes and improvements to logging and debugging.

1.3 (2015-01-13)
----------------

- Version 1.2 added a new 'Importer' storage backend but it had 2 bugs.

  - An interrupted migration could not be resumed.
  - Merging several ZODB only worked if NEO could import all classes used by
    the application. This has been fixed by repickling without loading any
    object.

- Logging has been improved for a better integration with the environment:

  - RTMIN+1 signal was changed to reopen logs. RTMIN+1 & RTMIN+2 signals, which
    were previously used for debugging, have been remapped to RTMIN+2 & RTMIN+3
  - In Zope, client registers automatically for log rotation (USR2).
  - NEO logs are SQLite DB that are not open anymore with a persistent journal,
    because this is incompatible with the rename+reopen way to rotate logs,
    and we want to support logrotate.
  - 'neolog' can now open gzip/bz2 compressed logs transparently.
  - 'neolog' does not spam the console anymore when piped to a process that
    exits prematurely.

- MySQL backend has been updated to work with recent MariaDB (>=10).
- 2 'neomaster' command-line options were added to set upstream cluster/masters.

1.2 (2014-07-30)
----------------

The most important changes in this version are the work about conversion of
databases from/to NEO:

- A new 'Importer' storage backend has been implemented and this is now the
  recommended way to migrate existing Zope databases. See 'importer.conf'
  example file for more information.
- 'neomigrate' command refused to run since version 1.0
- Exported data serials by NEO iterator were wrong. There are still differences
  with FileStorage:

  - NEO always resolves to original serial, to avoid any indirection
    (which slightly speeds up undo at the expense of a more complex pack code)
  - NEO does not make any difference between object deletion and creation undone
    (data serial always null in storage)

  Apart from that, conversion of database back from NEO should be fixed.

Other changes are:

- A warning was added in 'neo.conf' about a possible misuse of replicas.
- Compatibility with Python 2.6 has been dropped.
- Support for recent version of SQlite has been added.
- A memory leak has been fixed in replication.
- MySQL backend now fails instead of silently reconnecting if there is any
  pending change, which could cause data loss.
- Optimization and minor bugfixes.

1.1 (2014-01-07)
----------------

- Client failed at reconnecting properly to master. It could kill the master
  (during tpc_finish!) or end up with invalid caches (i.e. possible data
  corruption). Now, connection to master is even optional between
  transaction.begin() and tpc_begin, as long as partition table contains
  up-to-date data.
- Compatibility with ZODB 3.9 has been dropped. Only 3.10.x branch is supported.
- checkCurrentSerialInTransaction was not working.
- Optimization and minor bugfixes.

1.0 (2012-08-28)
----------------

This version mainly comes with stabilized SQL tables format and efficient backup
feature, relying on replication, which has been fully reimplemented:

- It is now incremental, instead of being done on whole partitions.
  Schema of MySQL tables have been changed in order to optimize storage layout,
  for good partial replication performance.
- It runs at lowest priority not to degrade performance for client nodes.
- A cluster in the new BACKINGUP state is a client to a normal cluster and all
  its storage nodes are notified of invalidations and replicate from upstream
  nodes.

Other changes are:

- Compatibility with Python < 2.6 and ZODB < 3.9 has been dropped.
- Cluster is now automatically started when all storage nodes of UP_TO_DATE
  cells are available, similarly to ``mdadm assemble --no-degraded`` behaviour.
- NEO learned to check replicas, to detect data corruption or bugs during
  replication. When done on a backup cluster, upstream data is used as
  reference. This is still limited to data indexes (tid & oid/serial).
- NEO logs now are SQLite DB that always contain all debugging information
  including exchanged packets. Records are first kept in RAM, at most 16 MB by
  default, and there are flushed to disk only upon RTMIN signal or any important
  record. A 'neolog' script has been written to help reading such DB.
- Master addresses must be separated by spaces. '/' can't be used anymore.
- Adding and removing master nodes is now easier: unknown incoming master nodes
  are now accepted instead of rejected, and nodes can be given a path to a file
  that maintains a list of known master nodes.
- Node UUIDs have been shortened from 16 to 4 bytes, for better performance and
  easier debugging.

Also contains code clean-ups and bugfixes.

0.10.1 (2012-03-13)
-------------------

- Client didn't limit its memory usage when committing big transactions.
- Master failed to disconnect clients when cluster leaves RUNNING state.

0.10 (2011-10-17)
-----------------

- Storage was unable or slow to process large-sized transactions.
  This required to change protocol and MySQL tables format.
- NEO learned to store empty values (although it's useless when managed by
  a ZODB Connection).

0.9.2 (2011-10-17)
------------------

- storage: a specific socket can be given to MySQL backend
- storage: a ConflictError could happen when client is much faster than master
- 'verbose' command line option of 'neomigrate' did not work
- client: ZODB monkey-patch randomly raised a NameError

0.9.1 (2011-09-24)
------------------

- client: method to retrieve history of persistent objects was incompatible
  with recent ZODB and needlessly asked all storages systematically.
- neoctl: 'print node' command (to get list of all nodes) raised an
  AssertionError.
- 'neomigrate' raised a TypeError when converting NEO DB back to FileStorage.

0.9 (2011-09-12)
----------------

Initial release.

NEO is considered stable enough to replace existing ZEO setups, except that:

- there's no backup mechanism (aka efficient snapshoting): there's only
  replication and underlying MySQL tools

- MySQL tables format may change in the future
