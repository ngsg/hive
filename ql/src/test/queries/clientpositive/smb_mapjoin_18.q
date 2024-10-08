--! qt:dataset:src
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;


set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 
set hive.cbo.fallback.strategy=NEVER;

-- Create two bucketed and sorted tables
CREATE TABLE test_table1_n7 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2_n7 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n7 PARTITION (ds = '1') SELECT *;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation
EXPLAIN
INSERT OVERWRITE TABLE test_table2_n7 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1_n7 a WHERE a.ds = '1';

INSERT OVERWRITE TABLE test_table2_n7 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1_n7 a WHERE a.ds = '1';

select count(*) from test_table1_n7 where ds = '1';
select count(*) from test_table1_n7 where ds = '1' and hash(key) % 2 = 0;
select count(*) from test_table1_n7 where ds = '1' and hash(key) % 2 = 1;
select count(*) from test_table1_n7 tablesample (bucket 1 out of 2) s where ds = '1';
select count(*) from test_table1_n7 tablesample (bucket 2 out of 2) s where ds = '1';

select count(*) from test_table2_n7 where ds = '1';
select count(*) from test_table2_n7 where ds = '1' and hash(key) % 2 = 0;
select count(*) from test_table2_n7 where ds = '1' and hash(key) % 2 = 1;
select count(*) from test_table2_n7 tablesample (bucket 1 out of 2) s where ds = '1';
select count(*) from test_table2_n7 tablesample (bucket 2 out of 2) s where ds = '1';

set hive.optimize.constant.propagation=false;
-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation, one of the buckets should be empty

EXPLAIN
INSERT OVERWRITE TABLE test_table2_n7 PARTITION (ds = '2')
SELECT a.key, a.value FROM test_table1_n7 a WHERE a.ds = '1' and a.key = 238;

INSERT OVERWRITE TABLE test_table2_n7 PARTITION (ds = '2')
SELECT a.key, a.value FROM test_table1_n7 a WHERE a.ds = '1' and a.key = 238;

set hive.optimize.constant.propagation=true;
select count(*) from test_table2_n7 where ds = '2';
select count(*) from test_table2_n7 where ds = '2' and hash(key) % 2 = 0;
select count(*) from test_table2_n7 where ds = '2' and hash(key) % 2 = 1;
select count(*) from test_table2_n7 tablesample (bucket 1 out of 2) s where ds = '2';
select count(*) from test_table2_n7 tablesample (bucket 2 out of 2) s where ds = '2';

EXPLAIN
INSERT OVERWRITE TABLE test_table2_n7 PARTITION (ds = '3')
SELECT a.key, a.value FROM test_table2_n7 a WHERE a.ds = '2';

INSERT OVERWRITE TABLE test_table2_n7 PARTITION (ds = '2')
SELECT a.key, a.value FROM test_table2_n7 a WHERE a.ds = '2';

select count(*) from test_table2_n7 where ds = '3';
select count(*) from test_table2_n7 where ds = '3' and hash(key) % 2 = 0;
select count(*) from test_table2_n7 where ds = '3' and hash(key) % 2 = 1;
select count(*) from test_table2_n7 tablesample (bucket 1 out of 2) s where ds = '3';
select count(*) from test_table2_n7 tablesample (bucket 2 out of 2) s where ds = '3';
