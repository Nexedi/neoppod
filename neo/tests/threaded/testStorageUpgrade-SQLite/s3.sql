BEGIN TRANSACTION;
CREATE TABLE config (
                 name TEXT NOT NULL PRIMARY KEY,
                 value TEXT) WITHOUT ROWID
            ;
INSERT INTO "config" VALUES('name','testStorageUpgrade');
INSERT INTO "config" VALUES('nid','3');
INSERT INTO "config" VALUES('partitions','3');
INSERT INTO "config" VALUES('replicas','1');
INSERT INTO "config" VALUES('ptid','8');
CREATE TABLE data (
                 id INTEGER PRIMARY KEY,
                 hash BLOB NOT NULL,
                 compression INTEGER NOT NULL,
                 value BLOB NOT NULL);
CREATE TABLE obj (
                 partition INTEGER NOT NULL,
                 oid INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 data_id INTEGER,
                 value_tid INTEGER,
                 PRIMARY KEY (partition, tid, oid));
CREATE TABLE pt (
                 rid INTEGER NOT NULL,
                 nid INTEGER NOT NULL,
                 state INTEGER NOT NULL,
                 PRIMARY KEY (rid, nid));
INSERT INTO "pt" VALUES(0,1,0);
INSERT INTO "pt" VALUES(0,2,0);
INSERT INTO "pt" VALUES(1,1,0);
INSERT INTO "pt" VALUES(2,2,0);
INSERT INTO "pt" VALUES(1,3,0);
INSERT INTO "pt" VALUES(2,3,0);
CREATE TABLE tobj (
                 partition INTEGER NOT NULL,
                 oid INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 data_id INTEGER,
                 value_tid INTEGER,
                 PRIMARY KEY (tid, oid)
            ) WITHOUT ROWID
            ;
CREATE TABLE trans (
                 partition INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids BLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid INTEGER NOT NULL,
                 PRIMARY KEY (partition, tid)
            ) WITHOUT ROWID
            ;
CREATE TABLE ttrans (
                 partition INTEGER NOT NULL,
                 tid INTEGER NOT NULL,
                 packed BOOLEAN NOT NULL,
                 oids BLOB NOT NULL,
                 user BLOB NOT NULL,
                 description BLOB NOT NULL,
                 ext BLOB NOT NULL,
                 ttid INTEGER NOT NULL);
CREATE INDEX _obj_i1 ON
                 obj(partition, oid, tid)
          ;
CREATE INDEX _obj_i2 ON
                 obj(data_id)
          ;
COMMIT;
