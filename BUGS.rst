Although NEO is considered ready for production use in most cases, there are
a few bugs to know because they concern basic features of ZODB (marked with Z),
or promised features of NEO (marked with N).

All the listed bugs will be fixed with high priority.

(Z) Conflict resolution not fully implemented
---------------------------------------------

Even with a single storage node, so-called 'deadlock avoidance' may
happen to in order to resolve conflicts. In such cases, conflicts will not be
resolved even if your _p_resolveConflict() method would succeed, leading to a
normal ConflictError.

Although this should happen rarely enough not to affect performance, this can
be an issue if your application can't afford restarting the transaction,
e.g. because it interacted with external environment.

(Z) Storage nodes rejecting clients after other clients got disconnected from the master
----------------------------------------------------------------------------------------

Client nodes ignore the state of the connection to the master node when reading
data from storage, as long as their partition tables are recent enough. This
way, they are able to finish read-only transactions even if they can't reach
the master, which may be useful for high availability. The downside is that the
master node ignores their node ids are still used, which causes "uuid"
conflicts when reallocating them.

Since an unused NEO Storage should not insist in staying connected to master
node, solutions are:

- change the way node ids are generated (either random or always increasing),
- or make storage nodes reject clients that aren't known by the master, but
  this drops the ability to perform read-only operations as described above
  (a client that is not connected to the master would be forced to reconnect
  before any query to the storage).

Workaround: restart clients that aren't connected to the master

(N) Storage failure or update may lead to POSException or break undoLog()
-------------------------------------------------------------------------

Storage nodes are only queried once at most and if all (for the requested
partition) failed, the client raises instead of asking the master whether it
had an up-to-date partition table (and retry if useful).

In the case of undoLog(), incomplete results may be returned.

(N) Storage does not discard answers from aborted replications
--------------------------------------------------------------

In some cases, this can lead to data corruption (wrong AnswerFetch*) or crashes
(e.g. KeyError because self.current_partition is None at the beginning of
Replicator.fetchObjects).

The assumption that aborting the replication of a partition implies the closure
of the connection turned out to be wrong, e.g. when a partition is aborted by a
third party, like CORRUPTED/DISCARDED event from the master.

Workaround: do not replicate or tweak while checking replicas.

Currently, this can be reproduced by running testBackupNodeLost
(neo.tests.threaded.testReplication.ReplicationTests) many times.

(N) A backup cell may be wrongly marked as corrupted while checking replicas
----------------------------------------------------------------------------

This happens in the following conditions:

1. a backup cluster starts to check replicas whereas a cell is outdated
2. this cell becomes updated, but only up to a tid smaller than the max tid
   to check (this can't happen for a non-backup cluster)
3. the cluster actually starts to check the related partition
4. the cell is checked completely before it could replicate up to the max tid
   to check

Workaround: make sure all cells are up-to-date before checking replicas.

Found by running testBackupNodeLost many times.
