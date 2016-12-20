
DROP TABLE tbl1;
DROP TABLE tbl2;
CREATE TABLE tbl1(a bigint, b bigint, c bigint, d bigint, e bigint);
CREATE TABLE tbl2(a bigint, b bigint, c bigint, d bigint, e bigint);
LOAD DATA LOCAL INPATH '../data/files/md_join_test_tbl1.txt' INTO TABLE tbl1;
LOAD DATA LOCAL INPATH '../data/files/md_join_test_tbl2.txt' INTO TABLE tbl2;

set hive.map.aggr=false;
set hive.groupby.skewindata=false;

EXPLAIN
select tt2.e, count(t.c1c), count(distinct t.cd2d), sum(tt2.b) from (select t1.e as ee, count(t1.c) as c1c, count(distinct t2.d) as cd2d from tbl1 t1 join tbl2 t2 on (t1.a=t2.a) group by t1.e) t join tbl2 tt2 on (t.ee=tt2.e) group by tt2.e;

select tt2.e, count(t.c1c), count(distinct t.cd2d), sum(tt2.b) from (select t1.e as ee, count(t1.c) as c1c, count(distinct t2.d) as cd2d from tbl1 t1 join tbl2 t2 on (t1.a=t2.a) group by t1.e) t join tbl2 tt2 on (t.ee=tt2.e) group by tt2.e;


set hive.map.aggr=false;
set hive.groupby.skewindata=true;

EXPLAIN
select tt2.e, count(t.c1c), count(distinct t.cd2d), sum(tt2.b) from (select t1.e as ee, count(t1.c) as c1c, count(distinct t2.d) as cd2d from tbl1 t1 join tbl2 t2 on (t1.a=t2.a) group by t1.e) t join tbl2 tt2 on (t.ee=tt2.e) group by tt2.e;

select tt2.e, count(t.c1c), count(distinct t.cd2d), sum(tt2.b) from (select t1.e as ee, count(t1.c) as c1c, count(distinct t2.d) as cd2d from tbl1 t1 join tbl2 t2 on (t1.a=t2.a) group by t1.e) t join tbl2 tt2 on (t.ee=tt2.e) group by tt2.e;


set hive.map.aggr=true;
set hive.groupby.skewindata=true;

EXPLAIN
select tt2.e, count(t.c1c), count(distinct t.cd2d), sum(tt2.b) from (select t1.e as ee, count(t1.c) as c1c, count(distinct t2.d) as cd2d from tbl1 t1 join tbl2 t2 on (t1.a=t2.a) group by t1.e) t join tbl2 tt2 on (t.ee=tt2.e) group by tt2.e;

select tt2.e, count(t.c1c), count(distinct t.cd2d), sum(tt2.b) from (select t1.e as ee, count(t1.c) as c1c, count(distinct t2.d) as cd2d from tbl1 t1 join tbl2 t2 on (t1.a=t2.a) group by t1.e) t join tbl2 tt2 on (t.ee=tt2.e) group by tt2.e;


set hive.map.aggr=true;
set hive.groupby.skewindata=false;

EXPLAIN
select tt2.e, count(t.c1c), count(distinct t.cd2d), sum(tt2.b) from (select t1.e as ee, count(t1.c) as c1c, count(distinct t2.d) as cd2d from tbl1 t1 join tbl2 t2 on (t1.a=t2.a) group by t1.e) t join tbl2 tt2 on (t.ee=tt2.e) group by tt2.e;

select tt2.e, count(t.c1c), count(distinct t.cd2d), sum(tt2.b) from (select t1.e as ee, count(t1.c) as c1c, count(distinct t2.d) as cd2d from tbl1 t1 join tbl2 t2 on (t1.a=t2.a) group by t1.e) t join tbl2 tt2 on (t.ee=tt2.e) group by tt2.e;


DROP TABLE tbl1;
DROP TABLE tbl2;

