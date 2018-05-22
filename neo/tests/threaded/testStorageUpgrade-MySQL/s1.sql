CREATE TABLE `bigdata` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `value` mediumblob NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
CREATE TABLE `config` (
  `name` varbinary(255) NOT NULL,
  `value` varbinary(255) DEFAULT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
INSERT INTO `config` VALUES ('name','testStorageUpgrade'),('nid','1'),('partitions','3'),('ptid','9'),('replicas','1');
CREATE TABLE `data` (
  `id` bigint(20) unsigned NOT NULL,
  `hash` binary(20) NOT NULL,
  `compression` tinyint(3) unsigned DEFAULT NULL,
  `value` mediumblob NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
INSERT INTO `data` VALUES (0,0x0BEEC7B5EA3F0FDBC95D0DD47F3C5BC275DA8A33,0,0x666F6F);
CREATE TABLE `obj` (
  `partition` smallint(5) unsigned NOT NULL,
  `oid` bigint(20) unsigned NOT NULL,
  `tid` bigint(20) unsigned NOT NULL,
  `data_id` bigint(20) unsigned DEFAULT NULL,
  `value_tid` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`partition`,`tid`,`oid`),
  KEY `partition` (`partition`,`oid`,`tid`),
  KEY `data_id` (`data_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
INSERT INTO `obj` VALUES (0,0,231616946283203125,0,NULL);
CREATE TABLE `pt` (
  `rid` int(10) unsigned NOT NULL,
  `nid` int(11) NOT NULL,
  `state` tinyint(3) unsigned NOT NULL,
  PRIMARY KEY (`rid`,`nid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
INSERT INTO `pt` VALUES (0,1,0),(0,2,0),(1,1,0),(1,3,1),(2,2,0),(2,3,1);
CREATE TABLE `tobj` (
  `partition` smallint(5) unsigned NOT NULL,
  `oid` bigint(20) unsigned NOT NULL,
  `tid` bigint(20) unsigned NOT NULL,
  `data_id` bigint(20) unsigned DEFAULT NULL,
  `value_tid` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`tid`,`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
CREATE TABLE `trans` (
  `partition` smallint(5) unsigned NOT NULL,
  `tid` bigint(20) unsigned NOT NULL,
  `packed` tinyint(1) NOT NULL,
  `oids` mediumblob NOT NULL,
  `user` blob NOT NULL,
  `description` blob NOT NULL,
  `ext` blob NOT NULL,
  `ttid` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`partition`,`tid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
INSERT INTO `trans` VALUES (1,231616946283203125,0,'\0\0\0\0\0\0\0\0','','','',231616946283203125);
CREATE TABLE `ttrans` (
  `partition` smallint(5) unsigned NOT NULL,
  `tid` bigint(20) unsigned NOT NULL,
  `packed` tinyint(1) NOT NULL,
  `oids` mediumblob NOT NULL,
  `user` blob NOT NULL,
  `description` blob NOT NULL,
  `ext` blob NOT NULL,
  `ttid` bigint(20) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
