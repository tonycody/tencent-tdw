drop table test_limit_op_1;
create table test_limit_op_1(tint_1 INT, tstr_1 STRING, tstr_2 STRING, tint_2 INT);
LOAD DATA LOCAL INPATH '../data/files/kv6.txt' INTO TABLE test_limit_op_1;

drop table test_limit_op_2;
create table test_limit_op_2(tint_1 INT, tstr_1 STRING, tstr_2 STRING, tint_2 INT) PARTITION by RANGE(tint_1) (partition range_1 VALUES LESS THAN (100));
from test_limit_op_1 insert table test_limit_op_2 select * where 
tint_1<100;
alter table  test_limit_op_2 add partition range_2 values less than (200);
from test_limit_op_1 insert table test_limit_op_2 select * where 
tint_1<200 and tint_1 > 100;

explain
select tint_1,tstr_1   from test_limit_op_1 limit 10;
explain
select tint_1,tstr_1   from test_limit_op_2 limit 10;

select tint_1,tstr_1   from test_limit_op_1 limit 10;
select tint_1,tstr_1   from test_limit_op_2 limit 10;

drop table test_limit_op_1;
drop table test_limit_op_2;
