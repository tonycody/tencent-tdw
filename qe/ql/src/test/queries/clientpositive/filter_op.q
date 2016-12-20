create table test_op_s1 (tint_1 INT, tstr_1 STRING, tstr_2 STRING, tint_2 INT);
LOAD DATA LOCAL INPATH '../data/files/kv6.txt' INTO TABLE  test_op_s1;

create table test_op_s2 (tint_1 INT, tstr_1 STRING);
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE  test_op_s2;

create table mytmp1(tsum INT, tavg INT, tcount INT);
create table mytmp2(tavg INT, tcount INT, tsum INT);

create table test_op_t1 (tint_1 INT, tstr_1 STRING);
create table test_op_t2 (tint_1 INT, tstr_1 STRING);
create table test_op_t3 (tint_1 INT, tstr_1 STRING);
create table test_op_t4 (tint_1 INT, tstr_1 STRING);

FROM test_op_s1
INSERT OVERWRITE TABLE test_op_t1
SELECT test_op_s1.tint_1,test_op_s1.tstr_1
where test_op_s1.tint_1>100 and test_op_s1.tint_1<200
order by test_op_s1.tint_1
INSERT OVERWRITE TABLE test_op_t2
SELECT test_op_s1.tint_1,test_op_s1.tstr_2
where test_op_s1.tint_1>100 and test_op_s1.tint_1<200
order by test_op_s1.tint_1
INSERT OVERWRITE TABLE test_op_t3
SELECT test_op_s1.tint_2,test_op_s1.tstr_1
where test_op_s1.tint_1>100 and test_op_s1.tint_1<200
order by test_op_s1.tint_2
INSERT OVERWRITE TABLE test_op_t4
SELECT test_op_s1.tint_2,test_op_s1.tstr_2
where test_op_s1.tint_1>100 and test_op_s1.tint_1<200
order by test_op_s1.tint_2;

FROM test_op_s1  
INSERT OVERWRITE TABLE mytmp1
SELECT sum(test_op_s1.tint_2), avg(test_op_s1.tint_2) as tmpavg, count(test_op_s1.tint_2)
where test_op_s1.tint_2 > 5  and test_op_s1.tint_2 < 20
GROUP BY test_op_s1.tint_2  sort by tmpavg  

INSERT OVERWRITE TABLE mytmp2
SELECT avg(test_op_s1.tint_2), count(test_op_s1.tint_2) as tmpcount, sum(test_op_s1.tint_2)
where test_op_s1.tint_2 <20 and  test_op_s1.tint_2 > 5
GROUP BY test_op_s1.tint_2   sort by tmpcount;

select * from test_op_t1 order by tint_1, tstr_1;
select * from test_op_t2 order by tint_1, tstr_1;
select * from test_op_t3 order by tint_1, tstr_1;
select * from test_op_t4 order by tint_1, tstr_1;
select * from mytmp1;
select * from mytmp2;
select * from test_op_s2  where test_op_s2.tint_1<100 and test_op_s2.tint_1>50 order by test_op_s2.tint_1;

drop table test_op_t1;
drop table test_op_t2;
drop table test_op_t3;
drop table test_op_t4;

drop table test_op_s1;
drop table test_op_s2;

drop table mytmp1;
drop table mytmp2;
