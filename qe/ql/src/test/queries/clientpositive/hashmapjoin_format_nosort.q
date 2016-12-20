set hive.mapjoin.numrows = 2;
set hive.optimize.hashmapjoin = true;
set Sorted Merge.Map.Join = false;
set hive.hashPartition.num = 2;

drop table dest_j1;

CREATE TABLE dest_j1(key INT, value STRING, val2 STRING) stored as formatfile;

drop table hashtable_1;
CREATE TABLE hashtable_1(key int, value string) partition by hashkey(key) stored as formatfile;

drop table hashtable_2;
CREATE TABLE hashtable_2(key int, value string) partition by hashkey(key) stored as formatfile;

insert overwrite table hashtable_1
select * from src;

insert overwrite table hashtable_2
select * from src;

EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.key, x.value, y.value
FROM hashtable_1 x JOIN hashtable_2 y ON (x.key = y.key);

INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.key, x.value, y.value
FROM hashtable_1 x JOIN hashtable_2 y ON (x.key = y.key);

select * from dest_j1 x order by x.key;

drop table dest_j1;
drop table hashtable_1;
drop table hashtable_2;

set hive.optimize.hashmapjoin = false;
set Sorted Merge.Map.Join = false;
set hive.hashPartition.num = 500;
