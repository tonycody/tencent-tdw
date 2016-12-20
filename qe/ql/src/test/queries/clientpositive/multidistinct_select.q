
DROP TABLE tbl1;
CREATE TABLE tbl1(a bigint, b bigint, c bigint, d bigint, e bigint);
LOAD DATA LOCAL INPATH '../data/files/md_join_test_tbl1.txt' INTO TABLE tbl1;

set hive.map.aggr=false;
set hive.groupby.skewindata=false;

EXPLAIN
select a, count(a), count(distinct a) from tbl1 group by a;
select a, count(a), count(distinct a) from tbl1 group by a;

EXPLAIN
select a, count(distinct a) from tbl1 group by a;
select a, count(distinct a) from tbl1 group by a;


set hive.map.aggr=false;
set hive.groupby.skewindata=true;

EXPLAIN
select a, count(a), count(distinct a) from tbl1 group by a;
select a, count(a), count(distinct a) from tbl1 group by a;

EXPLAIN
select a, count(distinct a) from tbl1 group by a;
select a, count(distinct a) from tbl1 group by a;


set hive.map.aggr=true;
set hive.groupby.skewindata=true;

EXPLAIN
select a, count(a), count(distinct a) from tbl1 group by a;
select a, count(a), count(distinct a) from tbl1 group by a;

EXPLAIN
select a, count(distinct a) from tbl1 group by a;
select a, count(distinct a) from tbl1 group by a;


set hive.map.aggr=true;
set hive.groupby.skewindata=false;

EXPLAIN
select a, count(a), count(distinct a) from tbl1 group by a;
select a, count(a), count(distinct a) from tbl1 group by a;

EXPLAIN
select a, count(distinct a) from tbl1 group by a;
select a, count(distinct a) from tbl1 group by a;


DROP TABLE tbl1;

