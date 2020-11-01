Although NEO is considered ready for production use in most cases, there are
a few bugs to know because they concern basic features of ZODB (marked with Z),
or promised features of NEO (marked with N).

All the listed bugs will be fixed with high priority.

(N) A backup cell may be wrongly marked as corrupted while checking replicas
----------------------------------------------------------------------------

This happens in the following conditions:

1. a backup cluster starts to check replicas whereas a cell is outdated
2. this cell becomes updated, but only up to a tid smaller than the max tid
   to check (this can't happen for a non-backup cluster)
3. the cluster actually starts to check the related partition
4. the cell is checked completely before it could replicate up to the max tid
   to check

Sometimes, it causes the master to crash::

    File "neo/lib/handler.py", line 72, in dispatch
      method(conn, *args, **kw)
    File "neo/master/handlers/storage.py", line 93, in notifyReplicationDone
      cell_list = app.backup_app.notifyReplicationDone(node, offset, tid)
    File "neo/master/backup_app.py", line 337, in notifyReplicationDone
      assert cell.isReadable()
  AssertionError

Workaround: make sure all cells are up-to-date before checking replicas.

Found by running testBackupNodeLost many times:

- either a failureException: 12 != 11
- or the above assert failure, in which case the unit test freezes
