drop table t1;
drop table t2;

create table t1 (key int, value string);
insert OVERWRITE TABLE t1
select key, value from src where key <= 10;

select * from t1 sort by key;

create table t2 (key int, value string);
insert OVERWRITE TABLE t2
select key, value from t1;

select * from t2 sort by key;

explain select * from (select key, value from t1 union select key, value from t2) u;
select * from (select key, value from t1 union select key, value from t2) u;

drop table t1;
drop table t2;
