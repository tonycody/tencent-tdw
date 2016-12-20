drop table table1;
create table table1  (col_name1 string, col_name2 string) partition by range(col_name1) subpartition by hashkey(col_name2) (partition par_name1 values less than (200), partition par_name2 values less than (300), partition default);
explain
insert overwrite table table1 select * from src;
insert overwrite table table1 select * from src;
select * from table1;
drop table table1;
