drop table tmp_insert_test;
drop table tmp_insert_test_p;

create table tmp_insert_test (key string, value string) stored as textfile;
load data local inpath '../data/files/kv1.txt' into table tmp_insert_test;

create table tmp_insert_test_p (key string, value string, ds string) partition by list(ds) 
(partition p0 values in ('2009-08-01'))
stored as textfile;

insert overwrite table tmp_insert_test_p select key, value, '2009-08-01' from tmp_insert_test;
select * from tmp_insert_test_p where ds= '2009-08-01' sort by key,value;
select count(1) from tmp_insert_test_p;

load data local inpath '../data/files/kv2.txt' into table tmp_insert_test;
select count(1) from tmp_insert_test;
insert overwrite table tmp_insert_test_p select key, value, '2009-08-01' from tmp_insert_test;
select * from tmp_insert_test_p where ds= '2009-08-01' sort by key,value;
select count(1) from tmp_insert_test_p;

drop table tmp_insert_test;
drop table tmp_insert_test_p;
