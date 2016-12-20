set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;


drop table table1;
drop table table11;
drop table table2;
drop table table22;
drop table table3;
drop table table33;

create table table1 (col_name1 int, col_name2 int) partition by hashkey(col_name2);
describe extended table1;
create table table11 like table1;
describe extended table11;

create table table2 (col_name1 int, col_name2 int) partition by list(col_name1) subpartition by hashkey(col_name2) (partition par_name1 values in (1,3,5), partition par_name2 values in (2,4,6), partition default);
describe extended table2;
create table table22 like table2;
describe extended table22;

create table table3 (col_name1 int, col_name2 int) partition by range(col_name1) subpartition by hashkey(col_name2) (partition par_name1 values less than (2), partition par_name2 values less than (5), partition default);
describe extended table3;
create table table33 like table3;
describe extended table33;

drop table table1;
drop table table2;
drop table table3;
drop table table11;
drop table table22;
drop table table33;

