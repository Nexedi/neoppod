NEO. Normal load on Woelfel for ~ 10 minutes. Top 10 SQL queries
----------------------------------------------------------------

    Count: 979463  Time=0.00s (393s)  Lock=0.00s (30s)  Rows_sent=1.0 (979463), Rows_examined=2.0 (1999173), Rows_affected=0.0 (0), root[root]@localhost
      SELECT tid, compression, data.hash, value, value_tid FROM obj LEFT JOIN data ON (obj.data_id = data.id) WHERE \`partition\` = N AND oid = N AND tid < N ORDER BY tid DESC LIMIT N

    Count: 8115  Time=0.01s (101s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.0 (0), Rows_affected=0.0 (0), root[root]@localhost
      commit

    Count: 979815  Time=0.00s (94s)  Lock=0.00s (20s)  Rows_sent=0.0 (163), Rows_examined=9.9 (9741734), Rows_affected=0.0 (0), root[root]@localhost
      SELECT tid FROM obj WHERE \`partition\`=N AND oid=N AND tid>N ORDER BY tid LIMIT N

    Count: 5755  Time=0.00s (27s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.7 (3868), Rows_affected=0.4 (2093), root[root]@localhost
      INSERT INTO trans SELECT * FROM ttrans WHERE tid=N

    Count: 5755  Time=0.00s (24s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.7 (3868), Rows_affected=0.4 (2093), root[root]@localhost
      DELETE FROM ttrans WHERE tid=N

    Count: 8  Time=1.51s (12s)  Lock=0.00s (0s)  Rows_sent=3.0 (24), Rows_examined=3.0 (24), Rows_affected=0.0 (0), root[root]@localhost
      SELECT \`partition\`, MAX(tid) FROM trans GROUP BY \`partition\`

    Count: 2094  Time=0.00s (9s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=1.3 (2652), Rows_affected=1.0 (2090), root[root]@localhost
      UPDATE ttrans SET tid=N WHERE ttid=N LIMIT N

    Count: 5670  Time=0.00s (7s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.0 (0), Rows_affected=0.9 (5000), root[root]@localhost
      INSERT INTO data VALUES (NULL, 'S', N, 'S')

    Count: 36  Time=0.19s (6s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.0 (0), Rows_affected=0.0 (0), root[root]@localhost
      DELETE FROM trans WHERE \`partition\`=N

    Count: 7755  Time=0.00s (4s)  Lock=0.00s (0s)  Rows_sent=0.8 (6555), Rows_examined=0.8 (6555), Rows_affected=0.0 (0), root[root]@localhost
      SELECT tid FROM obj WHERE \`partition\`=N AND oid=N ORDER BY tid DESC LIMIT N


NEO. Load on Woelfel + backup replication for ~ 10 minutes. Top 10 SQL queries
------------------------------------------------------------------------------

    Count: 1919  Time=0.32s (619s)  Lock=0.00s (0s)  Rows_sent=0.4 (799), Rows_examined=0.4 (799), Rows_affected=0.0 (0), root[root]@localhost
      SELECT tid FROM trans
      WHERE \`partition\` = N
      AND tid >= N AND tid <= N
      ORDER BY tid ASC LIMIT N

    Count: 110802  Time=0.00s (52s)  Lock=0.00s (3s)  Rows_sent=1.0 (110802), Rows_examined=2.1 (229296), Rows_affected=0.0 (0), root[root]@localhost
      SELECT tid, compression, data.hash, value, value_tid FROM obj LEFT JOIN data ON (obj.data_id = data.id) WHERE \`partition\` = N AND oid = N AND tid < N ORDER BY tid DESC LIMIT N

    Count: 3662  Time=0.01s (43s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.0 (0), Rows_affected=0.0 (0), root[root]@localhost
      commit

    Count: 2413  Time=0.00s (11s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.5 (1126), Rows_affected=0.3 (794), root[root]@localhost
      INSERT INTO trans SELECT * FROM ttrans WHERE tid=N

    Count: 113198  Time=0.00s (11s)  Lock=0.00s (2s)  Rows_sent=0.0 (47), Rows_examined=0.9 (105484), Rows_affected=0.0 (0), root[root]@localhost
      SELECT tid FROM obj WHERE \`partition\`=N AND oid=N AND tid>N ORDER BY tid LIMIT N

    Count: 2413  Time=0.00s (9s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.5 (1126), Rows_affected=0.3 (794), root[root]@localhost
      DELETE FROM ttrans WHERE tid=N

    Count: 794  Time=0.00s (3s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=1.1 (836), Rows_affected=1.0 (794), root[root]@localhost
      UPDATE ttrans SET tid=N WHERE ttid=N LIMIT N

    Count: 2275  Time=0.00s (3s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.0 (0), Rows_affected=1.0 (2180), root[root]@localhost
      INSERT INTO data VALUES (NULL, 'S', N, 'S')

    Count: 3472  Time=0.00s (2s)  Lock=0.00s (0s)  Rows_sent=0.8 (2808), Rows_examined=0.8 (2808), Rows_affected=0.0 (0), root[root]@localhost
      SELECT tid FROM obj WHERE \`partition\`=N AND oid=N ORDER BY tid DESC LIMIT N

    Count: 2413  Time=0.00s (1s)  Lock=0.00s (0s)  Rows_sent=0.0 (0), Rows_examined=0.9 (2286), Rows_affected=0.9 (2286), root[root]@localhost
      INSERT INTO obj SELECT \`partition\`, oid, N, data_id, value_tid  FROM tobj WHERE tid=N


Analysis
--------

- There were ~ 10^6 normal "data read" queries on load case. For load+backup they are now only ~ 10^5 (ten times less)
- In load+backup case the time is dominated by `SELECT tid FROM trans WHERE
  partiotion=N and N <= tid <= N ...` which is query behind `getReplicationTIDList()`
- That under-getReplicationTIDList() query is anomaly slow - **0.3s** on average
- Details from full slow log shows that query is usually fast (it uses primary
  key as index) but **sometimes** is extremely slow ~ **1.5** second::

    # User@Host: root[root] @ localhost []
    # Thread_id: 279  Schema: neo0  QC_hit: No
    # Query_time: 0.002586  Lock_time: 0.000072  Rows_sent: 1001  Rows_examined: 1001
    # Rows_affected: 0
    #
    # explain: id   select_type     table   type    possible_keys   key     key_len ref     rows    r_rows  filtered        r_filtered      Extra
    # explain: 1    SIMPLE          trans   range   PRIMARY         PRIMARY 10      NULL    1       1001.00 100.00  100.00  Using where; Using index
    #
    use neo0;
    SET timestamp=1474317299;
    SELECT tid FROM trans
                        WHERE `partition` = 0
                        AND tid >= 268534540462957022 AND tid <= 268534902497821053
                        ORDER BY tid ASC LIMIT 1001;

    ...

    # Time: 160919 22:35:01
    # User@Host: root[root] @ localhost []
    # Thread_id: 279  Schema: neo0  QC_hit: No
    # Query_time: 1.646955  Lock_time: 0.000049  Rows_sent: 81  Rows_examined: 81
    # Rows_affected: 0
    #
    # explain: id   select_type     table   type    possible_keys   key     key_len ref     rows    r_rows  filtered        r_filtered      Extra
    # explain: 1    SIMPLE          trans   range   PRIMARY         PRIMARY 10      NULL    81      81.00   75.31   100.00  Using where; Using index
    #
    use neo0;
    SET timestamp=1474317301;
    SELECT tid FROM trans
                        WHERE `partition` = 0
                        AND tid >= 268534852433453784 AND tid <= 268534902497821053
                        ORDER BY tid ASC LIMIT 1001;

    ...

    # Time: 160919 22:35:59
    # User@Host: root[root] @ localhost []
    # Thread_id: 279  Schema: neo0  QC_hit: No
    # Query_time: 1.604544  Lock_time: 0.000054  Rows_sent: 0  Rows_examined: 0
    # Rows_affected: 0
    #
    # explain: id   select_type     table   type    possible_keys   key     key_len ref     rows    r_rows  filtered        r_filtered      Extra
    # explain: 1    SIMPLE          trans   range   PRIMARY         PRIMARY 10      NULL    1       0.00    100.00  100.00  Using where; Using index
    #
    use neo0;
    SET timestamp=1474317359;
    SELECT tid FROM trans
                        WHERE `partition` = 0
                        AND tid >= 268534902497821054 AND tid <= 268534910087656108
                        ORDER BY tid ASC LIMIT 1001;


- Why this happens is question.


----------------------------------------

After upgrade to MariaDB 10.1.17::

    # Time: 160921 10:47:15
    # User@Host: root[root] @ localhost []
    # Thread_id: 6  Schema: neo1  QC_hit: No
    # Query_time: 4.118192  Lock_time: 0.000024  Rows_sent: 0  Rows_examined: 8476058
    # Rows_affected: 0
    #
    # explain: id   select_type     table   type    possible_keys       key             key_len ref             rows    r_rows          filtered        r_filtered      Extra
    # explain: 1    SIMPLE          obj     ref     PRIMARY,partition   partition       10      const,const     1       8476058.00      100.00          0.00            Using where; Using index
    #
    SET timestamp=1474447635;
    SELECT tid FROM obj WHERE `partition`=5 AND oid=79613 AND tid>268544235197088772 ORDER BY tid LIMIT 1;


    # a slow query caught:
    MariaDB [neo1]> show processlist;
    +----+------+-----------+------+---------+------+----------------------------+------------------------------------------------------------------------------------------------------+----------+
    | Id | User | Host      | db   | Command | Time | State                      | Info                                                                                                 | Progress |
    +----+------+-----------+------+---------+------+----------------------------+------------------------------------------------------------------------------------------------------+----------+
    |  3 | root | localhost | neo0 | Sleep   |    8 |                            | NULL                                                                                                 |    0.000 |
    |  4 | root | localhost | neo2 | Sleep   |   15 |                            | NULL                                                                                                 |    0.000 |
    |  5 | root | localhost | neo3 | Sleep   |   11 |                            | NULL                                                                                                 |    0.000 |
    |  6 | root | localhost | neo1 | Query   |    3 | Queried about 7710000 rows | SELECT tid FROM obj WHERE `partition`=5 AND oid=79613 AND tid>268544341634012678 ORDER BY tid LIMIT  |    0.000 |
    | 10 | root | localhost | neo1 | Query   |    0 | init                       | show processlist                                                                                     |    0.000 |
    +----+------+-----------+------+---------+------+----------------------------+------------------------------------------------------------------------------------------------------+----------+
    5 rows in set (0.00 sec)

    MariaDB [neo1]> show explain for 6;
    +------+-------------+-------+------+-------------------+-----------+---------+-------------+------+------------------------------------------+
    | id   | select_type | table | type | possible_keys     | key       | key_len | ref         | rows | Extra                                    |
    +------+-------------+-------+------+-------------------+-----------+---------+-------------+------+------------------------------------------+
    |    1 | SIMPLE      | obj   | ref  | PRIMARY,partition | partition | 10      | const,const |    1 | Using where; Using index; Using filesort |
    +------+-------------+-------+------+-------------------+-----------+---------+-------------+------+------------------------------------------+
    1 row in set, 1 warning (0.01 sec)

    # NOTE the difference:
    #   * type:         'ref' vs 'range'
    #   * key_len:      10 vs 18                (partition, oid)  vs  (partition, oid, tid)
    #   * ref:          const,const vs NULL
    #   * "Using filesort"

    MariaDB [neo1]> explain SELECT tid FROM obj WHERE `partition`=5 AND oid=79613 AND tid>268544341634012678 ORDER BY tid LIMIT 1;
    +------+-------------+-------+-------+-------------------+-----------+---------+------+------+--------------------------+
    | id   | select_type | table | type  | possible_keys     | key       | key_len | ref  | rows | Extra                    |
    +------+-------------+-------+-------+-------------------+-----------+---------+------+------+--------------------------+
    |    1 | SIMPLE      | obj   | range | PRIMARY,partition | partition | 18      | NULL |   24 | Using where; Using index |
    +------+-------------+-------+-------+-------------------+-----------+---------+------+------+--------------------------+
    1 row in set (0.00 sec)


----------------------------------------

( Analyzing backup slowness )

Fast case (XXX problem on neo0 ?)::

    AAA 268557222739722248 268557222739722247 1001 8
    0.000 ((1L, 'SIMPLE', None, None, None, None, None, None, None, None, None, None, 'no matching row in const table'),)
    Bytes_received                                                  :       289     +273
    Bytes_sent                                                      :       11155   +11144
    Com_select                                                      :       1       +1
    Com_show_status                                                 :       2       +1
    Handler_commit                                                  :       1       +1
    Queries                                                         :       259571  +2
    Questions                                                       :       3       +2
    Rows_sent                                                       :       1       +1
    Rows_tmp_read                                                   :       780     +780
    Table_locks_immediate                                           :       1       +1
    Tokudb_basement_deserialization_fixed_key                       :       2190354 +20
    Tokudb_basements_fetched_prefetch                               :       760731  +20
    Tokudb_basements_fetched_prefetch_bytes                         :       11024086528     +280064
    Tokudb_basements_fetched_prefetch_seconds                       :       1.96279 +4e-05
    Tokudb_leaf_decompression_to_memory_seconds                     :       218.702792      +0.001814
    Tokudb_leaf_deserialization_to_memory_seconds                   :       36.250267       +0.000219000000001
    Tokudb_txn_begin                                                :       259410  +2
    Tokudb_txn_commits                                              :       259382  +1
    (0.000)

Fast case (neo3)::

    AAA 268557254261782968 268557254363747334 1001 2
    0.001 ((1L, 'SIMPLE', 'trans', 'range', 'PRIMARY', 'PRIMARY', '10', None, 1L, 0.0, 100.0, 100.0, 'Using where; Using index'),)
    Bytes_received                                                  :       289     +273
    Bytes_sent                                                      :       11232   +11221
    Com_select                                                      :       1       +1
    Com_show_status                                                 :       2       +1
    Handler_commit                                                  :       1       +1
    Handler_read_key                                                :       1       +1
    Last_query_cost                                                 :       1.532333        +1.532333
    Queries                                                         :       699860  +2
    Questions                                                       :       3       +2
    Rows_read                                                       :       1       +1
    Rows_sent                                                       :       1       +1
    Rows_tmp_read                                                   :       780     +780
    Select_range                                                    :       1       +1
    Table_locks_immediate                                           :       1       +1
    Tokudb_basement_deserialization_fixed_key                       :       4243753 +1
    Tokudb_basements_fetched_target_query                           :       3215002 +1
    Tokudb_basements_fetched_target_query_bytes                     :       44778189312     +17408
    Tokudb_basements_fetched_target_query_seconds                   :       34.811946       +3.99999999701e-06
    Tokudb_cachetable_size_current                                  :       1081618155      +122560
    Tokudb_cachetable_size_leaf                                     :       1063377598      +122560
    Tokudb_leaf_decompression_to_memory_seconds                     :       373.862982      +0.000122999999974
    Tokudb_leaf_deserialization_to_memory_seconds                   :       63.778401       +2.50000000008e-05
    Tokudb_txn_begin                                                :       698705  +2
    Tokudb_txn_commits                                              :       698654  +1
    (0.000)



Slow case (neo0)::

    AAA 268557222739722248 268557226879226040 1001 0
    1.591 ((1L, 'SIMPLE', 'trans', 'range', 'PRIMARY', 'PRIMARY', '10', None, 1L, 1.0, 100.0, 100.0, 'Using where; Using index'),)
    Bytes_received                                                  :       289     +273
    Bytes_sent                                                      :       11194   +11183
    Com_select                                                      :       1       +1
    Com_show_status                                                 :       2       +1
    Handler_commit                                                  :       1       +1
    Handler_read_key                                                :       1       +1
    Handler_read_next                                               :       1       +1
    Innodb_background_log_sync                                      :       368     +2
    Innodb_master_thread_idle_loops                                 :       367     +2
    Last_query_cost                                                 :       2.742333        +2.742333
    Qcache_not_cached                                               :       16      +16
    Queries                                                         :       262341  +21
    Questions                                                       :       3       +2
    Rows_read                                                       :       2       +2
    Rows_sent                                                       :       2       +2
    Rows_tmp_read                                                   :       780     +780
    Select_range                                                    :       1       +1
    Slow_queries                                                    :       1       +1
    Table_locks_immediate                                           :       24      +24			XXX
    Threads_running                                                 :       1       +-1
    Tokudb_basement_deserialization_fixed_key                       :       2229717 +14386
    Tokudb_basements_fetched_target_query                           :       1215707 +14386
    Tokudb_basements_fetched_target_query_bytes                     :       16668472320     +152982528
    Tokudb_basements_fetched_target_query_seconds                   :       12.56421        +0.02178
    Tokudb_cachetable_cleaner_executions                            :       592     +2
    Tokudb_cachetable_evictions                                     :       9600    +51
    Tokudb_cachetable_pool_cachetable_total_items_processed         :       19415   +8
    Tokudb_cachetable_size_cachepressure                            :       8077172 +-83113
    Tokudb_cachetable_size_current                                  :       1144918868      +37576872	XXX
    Tokudb_cachetable_size_leaf                                     :       1131549588      +37662332
    Tokudb_cachetable_size_nonleaf                                  :       13368032        +-82340
    Tokudb_cachetable_size_rollback                                 :       1248    +-3120
    Tokudb_cursor_skip_deleted_leaf_entry                           :       796652814       +6456408
    Tokudb_filesystem_fsync_num                                     :       992     +1			XXX
    Tokudb_filesystem_fsync_time                                    :       9154018 +7358			XXX
    Tokudb_leaf_decompression_to_memory_seconds                     :       221.668309      +1.089154	XXX <-- !!!
    Tokudb_leaf_deserialization_to_memory_seconds                   :       36.807413       +0.189991	XXX <-- !!!
    Tokudb_leaf_node_full_evictions                                 :       9223    +49
    Tokudb_leaf_node_full_evictions_bytes                           :       319606176       +157852
    Tokudb_leaf_node_partial_evictions                              :       2212174 +12893
    Tokudb_leaf_node_partial_evictions_bytes                        :       174814689660    +1069000238
    Tokudb_locktree_memory_size                                     :       0       +-402
    Tokudb_locktree_sto_eligible_num                                :       0       +-5
    Tokudb_logger_next_lsn                                          :       2301973637      +1
    Tokudb_logger_writes                                            :       485     +1
    Tokudb_logger_writes_bytes                                      :       404674  +37
    Tokudb_logger_writes_seconds                                    :       0.00618 +1.3e-05
    Tokudb_logger_writes_uncompressed_bytes                         :       404674  +37
    Tokudb_nonleaf_node_full_evictions                              :       377     +2
    Tokudb_nonleaf_node_full_evictions_bytes                        :       56834699        +66212
    Tokudb_nonleaf_node_partial_evictions                           :       2389    +42
    Tokudb_nonleaf_node_partial_evictions_bytes                     :       1542323 +16128
    Tokudb_promotion_leaf_roots_injected_into                       :       1250    +1
    Tokudb_search_tries_gt_height                                   :       316     +3
    Tokudb_search_tries_gt_heightplus3                              :       315     +3
    Tokudb_total_search_retries                                     :       3315714 +31446			XXX <-- !!!
    Tokudb_txn_begin                                                :       262163  +18			XXX
    Tokudb_txn_commits                                              :       262138  +20
    Uptime                                                          :       369     +2
    Uptime_since_flush_status                                       :       2       +2
    (0.431)

----------------------------------------

::

    MariaDB [information_schema]> select table_schema, table_name, row_format, table_rows, avg_row_length, data_length, index_length, data_free, auto_increment, create_time, update_time, check_time from tables where table_schema like 'neo%' order by table_name, table_schema;
    +--------------+------------+------------+------------+----------------+-------------+--------------+-------------+----------------+---------------------+---------------------+------------+
    | table_schema | table_name | row_format | table_rows | avg_row_length | data_length | index_length | data_free   | auto_increment | create_time         | update_time         | check_time |
    +--------------+------------+------------+------------+----------------+-------------+--------------+-------------+----------------+---------------------+---------------------+------------+
    | neo0         | bigdata    | Dynamic    |          0 |              0 |           0 |            0 |       12288 |              1 | 2016-08-12 08:45:49 | 2016-08-12 08:45:49 | NULL       |
    | neo1         | bigdata    | Dynamic    |          0 |              0 |           0 |            0 |       12288 |              1 | 2016-08-15 10:34:35 | 2016-08-15 10:34:35 | NULL       |
    | neo2         | bigdata    | Dynamic    |          0 |              0 |           0 |            0 |       12288 |              1 | 2016-08-24 15:10:35 | 2016-08-24 15:10:35 | NULL       |
    | neo3         | bigdata    | Dynamic    |          0 |              0 |           0 |            0 |       12288 |              1 | 2016-08-24 15:10:35 | 2016-08-24 15:10:35 | NULL       |
    | neo0         | config     | Dynamic    |          7 |             47 |         332 |            0 |       12288 |           NULL | 2016-08-12 08:45:48 | 2016-09-23 23:43:31 | NULL       |
    | neo1         | config     | Dynamic    |          6 |             14 |          87 |            0 |       12288 |           NULL | 2016-08-15 10:34:34 | 2016-09-23 23:43:31 | NULL       |
    | neo2         | config     | Dynamic    |          6 |             14 |          87 |            0 |       12288 |           NULL | 2016-08-24 15:10:33 | 2016-09-23 23:43:31 | NULL       |
    | neo3         | config     | Dynamic    |          6 |             14 |          87 |            0 |       12288 |           NULL | 2016-08-24 15:10:33 | 2016-09-23 23:43:31 | NULL       |
    | neo0         | data       | Dynamic    |   16255962 |            837 | 13616514473 |    500663640 | 19836116992 |       73565233 | 2016-08-12 08:45:49 | 2016-09-27 08:58:32 | NULL       |
    | neo1         | data       | Dynamic    |   24899569 |            589 | 14672673448 |    763530620 |   791969792 |       44011519 | 2016-08-15 10:34:35 | 2016-09-27 08:58:32 | NULL       |
    | neo2         | data       | Dynamic    |   17675483 |            820 | 14506300363 |    547556937 |   168673280 |       20105887 | 2016-08-24 15:10:34 | 2016-09-27 08:58:32 | NULL       |
    | neo3         | data       | Dynamic    |   18977510 |            808 | 15338224987 |    588298253 |   125386752 |       21612879 | 2016-08-24 15:10:34 | 2016-09-27 08:59:31 | NULL       |
    | neo0         | obj        | Fixed      |   18448788 |             35 |   645745380 |   1168467340 |    22622208 |           NULL | 2016-09-22 18:01:22 | 2016-09-27 08:58:32 | NULL       |
    | neo1         | obj        | Fixed      |   27752111 |             35 |   971324317 |   1772084911 |    54521856 |           NULL | 2016-09-22 17:32:08 | 2016-09-27 08:58:32 | NULL       |
    | neo2         | obj        | Fixed      |   20044618 |             35 |   701561630 |   1302087095 |   517484544 |           NULL | 2016-08-25 08:27:41 | 2016-09-27 08:58:32 | NULL       |
    | neo3         | obj        | Fixed      |   21550351 |             35 |   754262285 |   1400527838 |   616304640 |           NULL | 2016-08-25 08:27:37 | 2016-09-27 08:59:31 | NULL       |
    | neo0         | pt         | Fixed      |         12 |             10 |         120 |            0 |       12288 |           NULL | 2016-09-20 22:26:18 | 2016-09-23 23:43:31 | NULL       |
    | neo1         | pt         | Fixed      |         12 |             10 |         120 |            0 |       12288 |           NULL | 2016-09-20 22:26:13 | 2016-09-23 23:43:31 | NULL       |
    | neo2         | pt         | Fixed      |         12 |             10 |         120 |            0 |       12288 |           NULL | 2016-09-20 22:26:11 | 2016-09-23 23:43:31 | NULL       |
    | neo3         | pt         | Fixed      |         12 |             10 |         120 |            0 |       12288 |           NULL | 2016-09-20 22:26:11 | 2016-09-23 23:43:31 | NULL       |
    | neo0         | tobj       | Fixed      |          0 |              0 |           0 |            0 |      110592 |           NULL | 2016-09-20 22:26:55 | 2016-09-27 08:58:32 | NULL       |
    | neo1         | tobj       | Fixed      |          0 |              0 |           0 |            0 |      110592 |           NULL | 2016-09-20 22:26:53 | 2016-09-27 08:58:32 | NULL       |
    | neo2         | tobj       | Fixed      |          0 |              0 |           0 |            0 |      110592 |           NULL | 2016-09-20 22:26:49 | 2016-09-27 08:58:32 | NULL       |
    | neo3         | tobj       | Fixed      |          0 |              0 |           0 |            0 |      110592 |           NULL | 2016-09-20 22:26:50 | 2016-09-27 08:59:32 | NULL       |
    | neo0         | trans      | Dynamic    |   25490462 |             54 |  1401504985 |            0 |   649662464 |           NULL | 2016-08-12 08:45:48 | 2016-09-27 08:58:32 | NULL       |
    | neo1         | trans      | Dynamic    |   13532423 |            104 |  1415025634 |            0 |   247037952 |           NULL | 2016-08-15 10:34:35 | 2016-09-27 08:59:31 | NULL       |
    | neo2         | trans      | Dynamic    |    8115659 |            130 |  1062100112 |            0 |    26652672 |           NULL | 2016-08-24 15:10:33 | 2016-09-27 08:58:32 | NULL       |
    | neo3         | trans      | Dynamic    |   10818318 |            130 |  1414551999 |            0 |    20656128 |           NULL | 2016-08-24 15:10:33 | 2016-09-27 08:58:32 | NULL       |
    | neo0         | ttrans     | Dynamic    |          0 |              0 |           0 |            0 |       12288 |           NULL | 2016-09-20 22:26:55 | 2016-09-27 08:58:32 | NULL       |
    | neo1         | ttrans     | Dynamic    |          0 |              0 |           0 |            0 |       12288 |           NULL | 2016-09-20 22:26:53 | 2016-09-27 08:59:31 | NULL       |
    | neo2         | ttrans     | Dynamic    |          0 |              0 |           0 |            0 |      110592 |           NULL | 2016-09-20 22:26:49 | 2016-09-27 08:58:32 | NULL       |
    | neo3         | ttrans     | Dynamic    |          0 |              0 |           0 |            0 |       12288 |           NULL | 2016-09-20 22:26:50 | 2016-09-27 08:58:32 | NULL       |
    +--------------+------------+------------+------------+----------------+-------------+--------------+-------------+----------------+---------------------+---------------------+------------+

XXX ^^^ neo0: 2x more rows, 2x less avg_row_length

::

    MariaDB [information_schema]> select count(*) from neo0.trans; select count(*) from neo1.trans; select count(*) from neo2.trans; select count(*) from neo3.trans;
    +----------+
    | count(*) |
    +----------+
    | 10816516 |
    +----------+
    1 row in set (8.43 sec)		NOTE same as in neo1 & neo3 but note the timings diff

    +----------+
    | count(*) |
    +----------+
    | 10819781 |
    +----------+
    1 row in set (5.70 sec)

    +----------+
    | count(*) |
    +----------+
    |  8115599 |
    +----------+
    1 row in set (2.92 sec)

    +----------+
    | count(*) |
    +----------+
    | 10818601 |
    +----------+
    1 row in set (4.22 sec)

----------------------------------------

NOTE::

    MariaDB [information_schema]> select PLUGIN_NAME,PLUGIN_VERSION,PLUGIN_TYPE,PLUGIN_AUTHOR,PLUGIN_DESCRIPTION,LOAD_OPTION,PLUGIN_MATURITY,PLUGIN_AUTH_VERSION from plugins where plugin_name like 'toku%';
    +-------------------------------+----------------+--------------------+---------------+----------------------------------------------------------------+-------------+-----------------+---------------------+
    | PLUGIN_NAME                   | PLUGIN_VERSION | PLUGIN_TYPE        | PLUGIN_AUTHOR | PLUGIN_DESCRIPTION                                             | LOAD_OPTION | PLUGIN_MATURITY | PLUGIN_AUTH_VERSION |
    +-------------------------------+----------------+--------------------+---------------+----------------------------------------------------------------+-------------+-----------------+---------------------+
    | TokuDB                        | 0.0            | STORAGE ENGINE     | Percona       | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology | ON          | Stable          | 5.6.31-77.0         |
    | TokuDB_trx                    | 0.0            | INFORMATION SCHEMA | Percona       | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology | ON          | Stable          | NULL                |
    | TokuDB_lock_waits             | 0.0            | INFORMATION SCHEMA | Percona       | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology | ON          | Stable          | NULL                |
    | TokuDB_locks                  | 0.0            | INFORMATION SCHEMA | Percona       | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology | ON          | Stable          | NULL                |
    | TokuDB_file_map               | 0.0            | INFORMATION SCHEMA | Percona       | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology | ON          | Stable          | NULL                |
    | TokuDB_fractal_tree_info      | 0.0            | INFORMATION SCHEMA | Percona       | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology | ON          | Stable          | NULL                |
    | TokuDB_fractal_tree_block_map | 0.0            | INFORMATION SCHEMA | Percona       | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology | ON          | Stable          | NULL                |
    | TokuDB_background_job_status  | 0.0            | INFORMATION SCHEMA | Percona       | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology | ON          | Stable          | NULL                |
    +-------------------------------+----------------+--------------------+---------------+----------------------------------------------------------------+-------------+-----------------+---------------------+

    MariaDB [information_schema]> select * from STATISTICS where table_schema like 'neo%' order by table_name, table_schema, column_name;
    +--------------+------------+------------+--------------+------------+--------------+-------------+-----------+-------------+----------+------------+
    | TABLE_SCHEMA | TABLE_NAME | NON_UNIQUE | INDEX_SCHEMA | INDEX_NAME | SEQ_IN_INDEX | COLUMN_NAME | COLLATION | CARDINALITY | NULLABLE | INDEX_TYPE |
    +--------------+------------+------------+--------------+------------+--------------+-------------+-----------+-------------+----------+------------+
    | neo0         | bigdata    |          0 | neo0         | PRIMARY    |            1 | id          | A         |           0 |          | BTREE      |
    | neo1         | bigdata    |          0 | neo1         | PRIMARY    |            1 | id          | A         |           0 |          | BTREE      |
    | neo2         | bigdata    |          0 | neo2         | PRIMARY    |            1 | id          | A         |           0 |          | BTREE      |
    | neo3         | bigdata    |          0 | neo3         | PRIMARY    |            1 | id          | A         |           0 |          | BTREE      |
    | neo0         | config     |          0 | neo0         | PRIMARY    |            1 | name        | A         |           7 |          | BTREE      |
    | neo1         | config     |          0 | neo1         | PRIMARY    |            1 | name        | A         |           6 |          | BTREE      |
    | neo2         | config     |          0 | neo2         | PRIMARY    |            1 | name        | A         |           6 |          | BTREE      |
    | neo3         | config     |          0 | neo3         | PRIMARY    |            1 | name        | A         |           6 |          | BTREE      |
    | neo0         | data       |          0 | neo0         | hash       |            2 | compression | A         |    16256132 | YES      | BTREE      |
    | neo0         | data       |          0 | neo0         | hash       |            1 | hash        | A         |    16256132 |          | BTREE      |
    | neo0         | data       |          0 | neo0         | PRIMARY    |            1 | id          | A         |    16256132 |          | BTREE      |
    | neo1         | data       |          0 | neo1         | hash       |            2 | compression | A         |    24903817 | YES      | BTREE      |
    | neo1         | data       |          0 | neo1         | hash       |            1 | hash        | A         |    24903817 |          | BTREE      |
    | neo1         | data       |          0 | neo1         | PRIMARY    |            1 | id          | A         |    24903817 |          | BTREE      |
    | neo2         | data       |          0 | neo2         | hash       |            2 | compression | A         |    17675663 | YES      | BTREE      |
    | neo2         | data       |          0 | neo2         | hash       |            1 | hash        | A         |    17675663 |          | BTREE      |
    | neo2         | data       |          0 | neo2         | PRIMARY    |            1 | id          | A         |    17675663 |          | BTREE      |
    | neo3         | data       |          0 | neo3         | hash       |            2 | compression | A         |    18977711 | YES      | BTREE      |
    | neo3         | data       |          0 | neo3         | hash       |            1 | hash        | A         |    18977711 |          | BTREE      |
    | neo3         | data       |          0 | neo3         | PRIMARY    |            1 | id          | A         |    18977711 |          | BTREE      |
    | neo0         | obj        |          1 | neo0         | data_id    |            1 | data_id     | A         |    18449031 | YES      | BTREE      |
    | neo0         | obj        |          0 | neo0         | PRIMARY    |            3 | oid         | A         |    18449031 |          | BTREE      |
    | neo0         | obj        |          0 | neo0         | partition  |            2 | oid         | A         |    18449031 |          | BTREE      |
    | neo0         | obj        |          0 | neo0         | partition  |            1 | partition   | A         |    18449031 |          | BTREE      |
    | neo0         | obj        |          0 | neo0         | PRIMARY    |            1 | partition   | A         |    18449031 |          | BTREE      |
    | neo0         | obj        |          0 | neo0         | PRIMARY    |            2 | tid         | A         |    18449031 |          | BTREE      |
    | neo0         | obj        |          0 | neo0         | partition  |            3 | tid         | A         |    18449031 |          | BTREE      |
    | neo1         | obj        |          1 | neo1         | data_id    |            1 | data_id     | A         |    27756440 | YES      | BTREE      |
    | neo1         | obj        |          0 | neo1         | PRIMARY    |            3 | oid         | A         |    27756440 |          | BTREE      |
    | neo1         | obj        |          0 | neo1         | partition  |            2 | oid         | A         |    27756440 |          | BTREE      |
    | neo1         | obj        |          0 | neo1         | PRIMARY    |            1 | partition   | A         |    27756440 |          | BTREE      |
    | neo1         | obj        |          0 | neo1         | partition  |            1 | partition   | A         |    27756440 |          | BTREE      |
    | neo1         | obj        |          0 | neo1         | PRIMARY    |            2 | tid         | A         |    27756440 |          | BTREE      |
    | neo1         | obj        |          0 | neo1         | partition  |            3 | tid         | A         |    27756440 |          | BTREE      |
    | neo2         | obj        |          1 | neo2         | data_id    |            1 | data_id     | A         |    20044932 | YES      | BTREE      |
    | neo2         | obj        |          0 | neo2         | PRIMARY    |            3 | oid         | A         |    20044932 |          | BTREE      |
    | neo2         | obj        |          1 | neo2         | partition  |            2 | oid         | A         |    20044932 |          | BTREE      |
    | neo2         | obj        |          0 | neo2         | PRIMARY    |            1 | partition   | A         |    20044932 |          | BTREE      |
    | neo2         | obj        |          1 | neo2         | partition  |            1 | partition   | A         |    20044932 |          | BTREE      |
    | neo2         | obj        |          1 | neo2         | partition  |            3 | tid         | A         |    20044932 |          | BTREE      |
    | neo2         | obj        |          0 | neo2         | PRIMARY    |            2 | tid         | A         |    20044932 |          | BTREE      |
    | neo3         | obj        |          1 | neo3         | data_id    |            1 | data_id     | A         |    21550769 | YES      | BTREE      |
    | neo3         | obj        |          0 | neo3         | PRIMARY    |            3 | oid         | A         |    21550769 |          | BTREE      |
    | neo3         | obj        |          1 | neo3         | partition  |            2 | oid         | A         |    21550769 |          | BTREE      |
    | neo3         | obj        |          1 | neo3         | partition  |            1 | partition   | A         |    21550769 |          | BTREE      |
    | neo3         | obj        |          0 | neo3         | PRIMARY    |            1 | partition   | A         |    21550769 |          | BTREE      |
    | neo3         | obj        |          0 | neo3         | PRIMARY    |            2 | tid         | A         |    21550769 |          | BTREE      |
    | neo3         | obj        |          1 | neo3         | partition  |            3 | tid         | A         |    21550769 |          | BTREE      |
    | neo0         | pt         |          0 | neo0         | PRIMARY    |            2 | nid         | A         |          12 |          | BTREE      |
    | neo0         | pt         |          0 | neo0         | PRIMARY    |            1 | rid         | A         |          12 |          | BTREE      |
    | neo1         | pt         |          0 | neo1         | PRIMARY    |            2 | nid         | A         |          12 |          | BTREE      |
    | neo1         | pt         |          0 | neo1         | PRIMARY    |            1 | rid         | A         |          12 |          | BTREE      |
    | neo2         | pt         |          0 | neo2         | PRIMARY    |            2 | nid         | A         |          12 |          | BTREE      |
    | neo2         | pt         |          0 | neo2         | PRIMARY    |            1 | rid         | A         |          12 |          | BTREE      |
    | neo3         | pt         |          0 | neo3         | PRIMARY    |            2 | nid         | A         |          12 |          | BTREE      |
    | neo3         | pt         |          0 | neo3         | PRIMARY    |            1 | rid         | A         |          12 |          | BTREE      |
    | neo0         | tobj       |          0 | neo0         | PRIMARY    |            2 | oid         | A         |           0 |          | BTREE      |
    | neo0         | tobj       |          0 | neo0         | PRIMARY    |            1 | tid         | A         |           0 |          | BTREE      |
    | neo1         | tobj       |          0 | neo1         | PRIMARY    |            2 | oid         | A         |           0 |          | BTREE      |
    | neo1         | tobj       |          0 | neo1         | PRIMARY    |            1 | tid         | A         |           0 |          | BTREE      |
    | neo2         | tobj       |          0 | neo2         | PRIMARY    |            2 | oid         | A         |           0 |          | BTREE      |
    | neo2         | tobj       |          0 | neo2         | PRIMARY    |            1 | tid         | A         |           0 |          | BTREE      |
    | neo3         | tobj       |          0 | neo3         | PRIMARY    |            2 | oid         | A         |           0 |          | BTREE      |
    | neo3         | tobj       |          0 | neo3         | PRIMARY    |            1 | tid         | A         |           0 |          | BTREE      |
    | neo0         | trans      |          0 | neo0         | PRIMARY    |            1 | partition   | A         |          37 |          | BTREE      |
    | neo0         | trans      |          0 | neo0         | PRIMARY    |            2 | tid         | A         |    25491101 |          | BTREE      |
    | neo1         | trans      |          0 | neo1         | PRIMARY    |            1 | partition   | A         |    13533241 |          | BTREE      |
    | neo1         | trans      |          0 | neo1         | PRIMARY    |            2 | tid         | A         |    13533241 |          | BTREE      |
    | neo2         | trans      |          0 | neo2         | PRIMARY    |            1 | partition   | A         |     8116159 |          | BTREE      |
    | neo2         | trans      |          0 | neo2         | PRIMARY    |            2 | tid         | A         |     8116159 |          | BTREE      |
    | neo3         | trans      |          0 | neo3         | PRIMARY    |            1 | partition   | A         |    10819556 |          | BTREE      |
    | neo3         | trans      |          0 | neo3         | PRIMARY    |            2 | tid         | A         |    10819556 |          | BTREE      |
    +--------------+------------+------------+--------------+------------+--------------+-------------+-----------+-------------+----------+------------+

    MariaDB [information_schema]> select * from table_constraints where table_schema like 'neo%' order by table_name, table_schema;
    +--------------------+-------------------+-----------------+--------------+------------+-----------------+
    | CONSTRAINT_CATALOG | CONSTRAINT_SCHEMA | CONSTRAINT_NAME | TABLE_SCHEMA | TABLE_NAME | CONSTRAINT_TYPE |
    +--------------------+-------------------+-----------------+--------------+------------+-----------------+
    | def                | neo0              | PRIMARY         | neo0         | bigdata    | PRIMARY KEY     |
    | def                | neo1              | PRIMARY         | neo1         | bigdata    | PRIMARY KEY     |
    | def                | neo2              | PRIMARY         | neo2         | bigdata    | PRIMARY KEY     |
    | def                | neo3              | PRIMARY         | neo3         | bigdata    | PRIMARY KEY     |
    | def                | neo0              | PRIMARY         | neo0         | config     | PRIMARY KEY     |
    | def                | neo1              | PRIMARY         | neo1         | config     | PRIMARY KEY     |
    | def                | neo2              | PRIMARY         | neo2         | config     | PRIMARY KEY     |
    | def                | neo3              | PRIMARY         | neo3         | config     | PRIMARY KEY     |
    | def                | neo0              | hash            | neo0         | data       | UNIQUE          |
    | def                | neo0              | PRIMARY         | neo0         | data       | PRIMARY KEY     |
    | def                | neo1              | hash            | neo1         | data       | UNIQUE          |
    | def                | neo1              | PRIMARY         | neo1         | data       | PRIMARY KEY     |
    | def                | neo2              | hash            | neo2         | data       | UNIQUE          |
    | def                | neo2              | PRIMARY         | neo2         | data       | PRIMARY KEY     |
    | def                | neo3              | hash            | neo3         | data       | UNIQUE          |
    | def                | neo3              | PRIMARY         | neo3         | data       | PRIMARY KEY     |
    | def                | neo0              | partition       | neo0         | obj        | UNIQUE          |
    | def                | neo0              | PRIMARY         | neo0         | obj        | PRIMARY KEY     |
    | def                | neo1              | partition       | neo1         | obj        | UNIQUE          |
    | def                | neo1              | PRIMARY         | neo1         | obj        | PRIMARY KEY     |
    | def                | neo2              | PRIMARY         | neo2         | obj        | PRIMARY KEY     |
    | def                | neo3              | PRIMARY         | neo3         | obj        | PRIMARY KEY     |
    | def                | neo0              | PRIMARY         | neo0         | pt         | PRIMARY KEY     |
    | def                | neo1              | PRIMARY         | neo1         | pt         | PRIMARY KEY     |
    | def                | neo2              | PRIMARY         | neo2         | pt         | PRIMARY KEY     |
    | def                | neo3              | PRIMARY         | neo3         | pt         | PRIMARY KEY     |
    | def                | neo0              | PRIMARY         | neo0         | tobj       | PRIMARY KEY     |
    | def                | neo1              | PRIMARY         | neo1         | tobj       | PRIMARY KEY     |
    | def                | neo2              | PRIMARY         | neo2         | tobj       | PRIMARY KEY     |
    | def                | neo3              | PRIMARY         | neo3         | tobj       | PRIMARY KEY     |
    | def                | neo0              | PRIMARY         | neo0         | trans      | PRIMARY KEY     |
    | def                | neo1              | PRIMARY         | neo1         | trans      | PRIMARY KEY     |
    | def                | neo2              | PRIMARY         | neo2         | trans      | PRIMARY KEY     |
    | def                | neo3              | PRIMARY         | neo3         | trans      | PRIMARY KEY     |
    +--------------------+-------------------+-----------------+--------------+------------+-----------------+
    34 rows in set (0.01 sec)



NOTE -> performance_schema      (requires server restart)
