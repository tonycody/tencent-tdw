set hive.ql.openwith.optimize=false;
drop table test_with_1;
CREATE TABLE test_with_1(
    i INT,
    b BIGINT,
    d DOUBLE
);
drop table test_with_2;
CREATE TABLE test_with_2(
    i INT,
    b BIGINT,
    d DOUBLE
);
drop table test_with_3;
CREATE TABLE test_with_3(
    i INT,
    b BIGINT,
    d DOUBLE
);

insert into test_with_1 values(1,1,1),(2,2,2),(3,3,3),(null,4,4),(5,null,null);
insert into test_with_2 values(1,12,1),(42,2,2),(3,23,13),(null,14,4),(5,null,null);
insert into test_with_3 values(1,1,11),(12,2,2),(73,3,3),(null,4,4),(15,null,null);

explain with a as (select * from test_with_1) select a.i from a;
with a as (select * from test_with_1) select a.i from a;

explain with a as (select * from test_with_1),bb as (select i,b  from test_with_2),c as (select i,d,b from test_with_3) select a.i,bb.b,c.d from a,bb,c;
with a as (select * from test_with_1),bb as (select i,b  from test_with_2),c as (select i,d,b from test_with_3) select a.i,bb.b,c.d from a,bb,c;

explain with a as (select * from test_with_1),bb as (select i,b,d  from test_with_2)  select a.b,a.d,a.i from a where not exists (select * from bb where bb.i=a.i);
with a as (select * from test_with_1),bb as (select i,b,d  from test_with_2)  select a.b,a.d,a.i from a where not exists (select * from bb where bb.i=a.i);

explain with a as (select * from test_with_1),bb as (select i,b  from test_with_2),c as (select i,d,b from test_with_3) select * from a join bb on (a.i=bb.i) join c on (bb.i=c.i) where a.d>0 and c.b>0;
with a as (select * from test_with_1),bb as (select i,b  from test_with_2),c as (select i,d,b from test_with_3) select * from a join bb on (a.i=bb.i) join c on (bb.i=c.i) where a.d>0 and c.b>0;

explain with a as (select * from test_with_1),bb as (select i,b,d  from test_with_2)  select a.b from a where exists (select * from bb where bb.i=a.i) group by a.b;
with a as (select * from test_with_1),bb as (select i,b,d  from test_with_2)  select a.b from a where exists (select * from bb where bb.i=a.i) group by a.b;

explain insert into table test_with_3 (i,b,d)  with a as (select * from test_with_1),bb as (select * from test_with_2) select a.i, bb.b,bb.d from a,bb;
insert into table test_with_3 (i,b,d)  with a as (select * from test_with_1),bb as (select * from test_with_2) select a.i, bb.b,bb.d from a,bb;

explain insert overwrite into table test_with_3   with a as (select * from test_with_1),bb as (select * from test_with_2)  select * from a where exists (select * from bb where bb.i=a.i)  and a.d>0;
insert overwrite into table test_with_3   with a as (select * from test_with_1),bb as (select * from test_with_2)  select * from a where exists (select * from bb where bb.i=a.i)  and a.d>0;

explain insert overwrite table test_with_3 select * from (select * from test_with_1) a  where exists (select * from (select * from test_with_2) bb where bb.i=a.i)  and a.d>0;

explain with a as (select * from test_with_1),bb as (select i,b,d  from a ) select i from bb;
with a as (select * from test_with_1),bb as (select i,b,d  from a ) select i from bb;

explain with a as (select * from test_with_1),bb as (select a.i,a.b,a.d from a left join test_with_2 union all select i,b,d from test_with_3) select sum(b) from bb group by bb.i;
with a as (select * from test_with_1),bb as (select a.i,a.b,a.d from a left join test_with_2 union all select i,b,d from test_with_3) select sum(b) from bb group by bb.i;

explain INSERT INTO TABLE test_with_3(b,i) WITH a AS (SELECT i ni ,ROW_NUMBER() OVER ( ORDER BY b ) nb   FROM test_with_1  WHERE  d >= 0) ,bb AS ( select i,b from  ( SELECT  i ,b FROM test_with_2  WHERE EXISTS (  SELECT *  FROM test_with_1 WHERE test_with_1.i = test_with_2.i) UNION ALL  SELECT ni i ,nb b  FROM a ) tmpt ) SELECT i,b FROM bb;

INSERT INTO TABLE test_with_3(b,i) WITH a AS (SELECT i ni ,ROW_NUMBER() OVER ( ORDER BY b ) nb   FROM test_with_1  WHERE  d >= 0) ,bb AS ( select i,b from  ( SELECT  i ,b FROM test_with_2  WHERE EXISTS (  SELECT *  FROM test_with_1 WHERE test_with_1.i = test_with_2.i) UNION ALL  SELECT ni i ,nb b  FROM a ) tmpt ) SELECT i,b FROM bb;

explain INSERT TABLE test_with_3( b ,i) SELECT i,b FROM (SELECT i,b  FROM (SELECT i ,b FROM test_with_2 WHERE EXISTS ( SELECT *  FROM test_with_1 WHERE test_with_1.i = test_with_2.i) UNION ALL SELECT ni i ,nb b FROM (SELECT i ni ,ROW_NUMBER() OVER ( ORDER BY b) nb FROM test_with_1 WHERE d >= 0) a) tmpt ) bb;

explain insert into table test_with_3 WITH a AS (SELECT distinct i,b,d FROM test_with_1) ,bb as (select a.i,a.b,a.d from test_with_2 join a),cc as (select bb.i,bb.b,bb.d from bb full join a on a.i=bb.i where a.i>0),dd as (select * from cc) select * from (select a.i,a.b,a.d from a join cc join (select * from dd) ff) ee;

insert into table test_with_3 WITH a AS (SELECT distinct i,b,d FROM test_with_1) ,bb as (select a.i,a.b,a.d from test_with_2 join a),cc as (select bb.i,bb.b,bb.d from bb full join a on a.i=bb.i where a.i>0),dd as (select * from cc) select * from (select a.i,a.b,a.d from a join cc join (select * from dd) ff) ee;

explain insert table test_with_3 select * from (select a.i,a.b,a.d from (SELECT distinct i ,b, d FROM test_with_1) a join (select bb.i,bb.b,bb.d from (select a.i,a.b,a.d from test_with_2 join (SELECT distinct i,b,d FROM test_with_1) a) bb full join (SELECT distinct i, b, d FROM test_with_1) a on a.i=bb.i where a.i>0) cc join (select * from (select * from (select bb.i,bb.b,bb.d from (select a.i,a.b,a.d from test_with_2 join (SELECT distinct i,b, d FROM test_with_1) a) bb full join (SELECT distinct i,b,d FROM test_with_1) a on a.i=bb.i where a.i>0) cc)  dd) ff) ee; 

explain insert into table test_with_3 WITH a AS (SELECT ROW_NUMBER () OVER (partition by i order by b) ii ,b,d FROM test_with_1),bb as (select test_with_2.i,test_with_2.b,test_with_2.d  from test_with_2 join a on a.b=test_with_2.b) select bb.i,bb.b,bb.d from bb where bb.i between 1 and 100;
insert into table test_with_3 WITH a AS (SELECT ROW_NUMBER () OVER (partition by i order by b) ii ,b,d FROM test_with_1),bb as (select test_with_2.i,test_with_2.b,test_with_2.d  from test_with_2 join a on a.b=test_with_2.b) select bb.i,bb.b,bb.d from bb where bb.i between 1 and 100;

explain insert table test_with_3  select bb.i,bb.b,bb.d from (select test_with_2.i,test_with_2.b,test_with_2.d  from test_with_2 join (SELECT ROW_NUMBER () OVER (partition by i order by b) ii ,b,d FROM test_with_1)a on a.b=test_with_2.b) bb where bb.i between 1 and 100;

explain insert into table test_with_3 WITH a AS (SELECT ROW_NUMBER () OVER (partition by i order by b) ii ,b,d FROM test_with_1),bb as (select test_with_2.i,test_with_2.b,test_with_2.d from test_with_2 where exists (select a.i,a.b,a.d from a where a.b=test_with_2.b and a.b between 1 and 100)) select bb.i,bb.b,bb.d from bb order by bb.i;
insert into table test_with_3 WITH a AS (SELECT ROW_NUMBER () OVER (partition by i order by b) ii ,b,d FROM test_with_1),bb as (select test_with_2.i,test_with_2.b,test_with_2.d from test_with_2 where exists (select a.i,a.b,a.d from a where a.b=test_with_2.b and a.b between 1 and 100)) select bb.i,bb.b,bb.d from bb order by bb.i;

explain insert table test_with_3  select bb.i,bb.b,bb.d from  (select test_with_2.i,test_with_2.b,test_with_2.d  from test_with_2 where exists (select a.ii,a.b,a.d from (SELECT ROW_NUMBER () OVER (partition by i order by b) ii,b,d FROM test_with_1) a where a.b=test_with_2.b and a.b between 1 and 100)) bb order by i;

explain insert into table test_with_3 WITH a AS (select i ii,sum(b) bb,sum(d) dd from test_with_1 where i >0 group by i),bbb as ( select  ROW_NUMBER () OVER (partition by ii order by bb) iii,bb,dd from a where dd between 0 and 1000 ) select bbb.iii,bbb.bb,sum(bbb.dd) from bbb where bbb.iii>0 group by iii,bb;
insert into table test_with_3 WITH a AS (select i ii,sum(b) bb,sum(d) dd from test_with_1 where i >0 group by i),bbb as ( select  ROW_NUMBER () OVER (partition by ii order by bb) iii,bb,dd from a where dd between 0 and 1000 ) select bbb.iii,bbb.bb,sum(bbb.dd) from bbb where bbb.iii>0 group by iii,bb;

explain insert table test_with_3  select bbb.iii,bbb.bb,sum(bbb.dd) from ( select ROW_NUMBER () OVER (partition by ii order by bb) iii,bb,dd from (select i ii,sum(b) bb,sum(d) dd from test_with_1 where i >0 group by i) a where dd between 0 and 1000 ) bbb where bbb.iii>0 group by iii,bb;

set hive.ql.openwith.optimize=true;
