drop table table1;
drop table table2;
set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

create table table1 (col_name1 int, col_name2 int) partition by list(col_name1) subpartition by hashkey(col_name2) (partition par_name1 values in (1,3,5), partition par_name2 values in (2,4,6), partition default);
create table table2 (col_name1 int, col_name2 int) partition by range(col_name1) subpartition by hashkey(col_name2) (partition par_name1 values less than (2), partition par_name2 values less than (5), partition default);
alter table table1 truncate partition (par_name1);
describe extended table1;
alter table table2 truncate partition (par_name2);
describe extended table2;
drop table table1;
drop table table2;
