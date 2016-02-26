NEO 1.6
=======

The `ttrans` table in MySQL/SQLite backends is automatically upgraded at
startup.

However, because the algorithm to verify unfinished data has changed in a way
that such data can not be migrated, a storage node will refuse to start to
if either `ttrans` or `tobj` are not empty.

Therefore, you must first stop NEO cleanly, before upgrading the code. If any
temporary table is not empty, you are requested to restart with an older
version of NEO.

The change in `ttrans` is such that NEO < 1.6 is still usable after a
successful upgrade to NEO >= 1.6, provided you always make sure that `ttrans`
and `tobj` are empty when switching from one to another.

NEO 1.4
=======

The schema of `data` table in MySQL/SQLite backends has changed and there is
no transparent migration because the changes are optional and this table may
be huge. You can either use the following SQL commands to upgrade each storage,
or migrate data to new nodes with replication.

MySQL
-----

::

  ALTER TABLE data
    -- minor optimization
    MODIFY value MEDIUMBLOB NOT NULL,
    -- fix store of multiple values that only differ by the compression flag
    DROP KEY hash, ADD UNIQUE (hash, compression);

SQLite
------

::

  -- In SQLite, ALTER TABLE is limited so it's all or nothing
  -- (here the added 'NOT NULL' on value column is cosmetic)
  CREATE TABLE new_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash BLOB NOT NULL,
    compression INTEGER NOT NULL,
    value BLOB NOT NULL);
  INSERT INTO new_data SELECT * FROM data;
  CREATE UNIQUE INDEX IF NOT EXISTS _data_i1 ON new_data(hash, compression);
  DROP TABLE data;
  ALTER TABLE new_data RENAME TO data;

NEO 1.0
=======

The format of MySQL tables has changed in NEO 1.0 and there is no backward
compatibility or transparent migration, so you will have to use the following
SQL commands to migrate each storage from NEO 0.10.x::

  -- make sure 'tobj' & 'ttrans' are empty first
  -- and all storages have up-to-date partition tables
  CREATE TABLE new_data (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, hash BINARY(20) NOT NULL UNIQUE, compression TINYINT UNSIGNED NULL, value LONGBLOB NULL) ENGINE = InnoDB SELECT DISTINCT obj.hash as hash FROM obj, data WHERE obj.hash=data.hash ORDER BY serial;
  UPDATE new_data, data SET new_data.compression=data.compression, new_data.value=data.value WHERE new_data.hash=data.hash;
  DROP TABLE data;
  RENAME TABLE new_data TO data;
  CREATE TABLE new_obj (partition SMALLINT UNSIGNED NOT NULL, oid BIGINT UNSIGNED NOT NULL, tid BIGINT UNSIGNED NOT NULL, data_id BIGINT UNSIGNED NULL, value_tid BIGINT UNSIGNED NULL, PRIMARY KEY (partition, tid, oid), KEY (partition, oid, tid), KEY (data_id)) ENGINE = InnoDB SELECT partition, oid, serial as tid, data.id as data_id, value_serial as value_tid FROM obj LEFT JOIN data ON (obj.hash=data.hash);
  DROP TABLE obj;
  RENAME TABLE new_obj TO obj;
  ALTER TABLE tobj CHANGE serial tid BIGINT UNSIGNED NOT NULL, CHANGE hash data_id BIGINT UNSIGNED NULL, CHANGE value_serial value_tid BIGINT UNSIGNED NULL;
  ALTER TABLE trans ADD COLUMN ttid BIGINT UNSIGNED NOT NULL;
  UPDATE trans SET ttid=tid;
  ALTER TABLE ttrans ADD COLUMN ttid BIGINT UNSIGNED NOT NULL;
  ALTER TABLE config MODIFY name VARBINARY(255) NOT NULL;
  CREATE TEMPORARY TABLE nid (new INT NOT NULL AUTO_INCREMENT PRIMARY KEY, old CHAR(32) NOT NULL, KEY (old)) ENGINE = InnoDB SELECT DISTINCT uuid as old FROM pt ORDER BY uuid;
  ALTER TABLE pt DROP PRIMARY KEY, ADD nid INT NOT NULL after rid;
  UPDATE pt, nid SET pt.nid=nid.new, state=state-1 WHERE pt.uuid=nid.old;
  ALTER TABLE pt DROP uuid, ADD PRIMARY KEY (rid, nid);
  UPDATE config, nid SET config.name='nid', config.value=nid.new WHERE config.name='uuid' AND nid.old=config.value;
  DELETE FROM config WHERE name='loid';

NEO 0.10
========

The format of MySQL tables has changed in NEO 0.10 and there is no backward
compatibility or transparent migration, so you will have to use the following
SQL commands to migrate each storage from NEO < 0.10::

  -- make sure 'tobj' is empty first
  DROP TABLE obj_short, tobj;
  ALTER TABLE obj CHANGE checksum hash BINARY(20) NULL;
  UPDATE obj SET value=NULL WHERE value='';
  UPDATE obj SET hash=UNHEX(SHA1(value));
  ALTER TABLE obj ADD KEY (hash(4));
  CREATE TABLE data (hash BINARY(20) NOT NULL PRIMARY KEY, compression TINYINT UNSIGNED NULL, value LONGBLOB NULL) ENGINE = InnoDB SELECT DISTINCT hash, compression, value FROM obj WHERE hash IS NOT NULL;
  ALTER TABLE obj DROP compression, DROP value;
  UPDATE obj, obj as undone SET obj.hash=undone.hash WHERE obj.value_serial IS NOT NULL AND obj.partition=undone.partition AND obj.oid=undone.oid AND obj.value_serial=undone.serial;

If 'tobj' is not empty, this means your cluster was not shutdown properly.
Restart it to flush the last committed transaction.
