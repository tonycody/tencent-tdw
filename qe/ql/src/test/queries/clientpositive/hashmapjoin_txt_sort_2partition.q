set hive.mapjoin.numrows = 2;
set hive.optimize.hashmapjoin = true;
set Sorted Merge.Map.Join = true;
set hive.hashPartition.num = 2;

drop table dest_j1;

CREATE TABLE dest_j1(insertdate STRING, key INT, value STRING) stored as inputformat 'org.apache.hadoop.mapred.TextInputFormat'outputformat 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

drop table hashtable_1;
CREATE TABLE hashtable_1(insertdate string, key int, value string) partition by list(INSERTDATE) subpartition by hashkey(key) (partition p1 values in ('2010-05-10'), partition default) stored as inputformat 'org.apache.hadoop.mapred.TextInputFormat'outputformat 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

drop table hashtable_2;
CREATE TABLE hashtable_2(insertdate string, key int, value string) partition by list(INSERTDATE) subpartition by hashkey(key) (partition p1 values in ('2010-05-10'), partition default) stored as inputformat 'org.apache.hadoop.mapred.TextInputFormat'outputformat 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

insert overwrite table hashtable_1
select "2010-05-10",* from src;

insert table hashtable_1
select "2010-05-11",* from src;

insert overwrite table hashtable_2
select "2010-05-10",* from src;

insert table hashtable_2
select "2010-05-11",* from src;

EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.insertdate, x.key, y.value
FROM hashtable_1 x JOIN hashtable_2 y ON (x.key = y.key) where x.insertdate="2010-05-10";

INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.insertdate, x.key, y.value
FROM hashtable_1 x JOIN hashtable_2 y ON (x.key = y.key) where x.insertdate="2010-05-10";

select * from dest_j1 x order by x.key;

EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.insertdate, x.key, y.value
FROM hashtable_1 x JOIN hashtable_2 y ON (x.key = y.key) where x.insertdate="2010-05-10" and y.insertdate="2010-05-10";

INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.insertdate, x.key, y.value
FROM hashtable_1 x JOIN hashtable_2 y ON (x.key = y.key) where x.insertdate="2010-05-10" and y.insertdate="2010-05-10";

select * from dest_j1 x order by x.key;

drop table dest_j1;
drop table hashtable_1;
drop table hashtable_2;

set hive.optimize.hashmapjoin = false;
set Sorted Merge.Map.Join = false;
set hive.hashPartition.num = 500;
