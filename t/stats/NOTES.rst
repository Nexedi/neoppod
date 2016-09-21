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
