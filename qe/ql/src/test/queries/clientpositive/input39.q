drop table t1;
drop table t2;

create table t1(key string, value string, ds string) partition by list(ds)
(partition p0 values in ('1'),
partition p1 values in ('2'),
partition p2 values in ('3'));

create table t2(key string, value string, ds string) partition by list(ds)
(partition p0 values in ('1'),
partition p1 values in ('2'),
partition p2 values in ('3'));

insert overwrite table t1
select key, value, '1' from src;

select count(1) from t1;

insert overwrite table t1
select key, value, '2' from src;

insert overwrite table t2
select key, value, '1' from src;

select count(1) from t2;

set hive.test.mode=true;
set hive.mapred.mode=strict;

explain
select count(1) from t1 join t2 on t1.key=t2.key where t1.ds='2' and t2.ds='1';

select count(1) from t1 join t2 on t1.key=t2.key where t1.ds='2' and t2.ds='1';


set hive.test.mode=false;

drop table t1;
drop table t2;
