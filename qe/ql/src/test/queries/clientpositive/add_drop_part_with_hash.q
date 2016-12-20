drop table table1;
drop table table2;

create table table1 (col_name1 int, col_name2 int) partition by list(col_name1) subpartition by hashkey(col_name2) (partition par_name1 values in (1,3,5), partition par_name2 values in (2,4,6), partition default);
show partitions table1;
alter table table1 add partition par_name3 values in (7,8,9);
show partitions table1;
alter table table1 drop partition (par_name3);
show partitions table1;

create table table2 (col_name1 int, col_name2 int) partition by range(col_name1) subpartition by hashkey(col_name2) (partition par_name1 values less than (2), partition par_name2 values less than (5), partition default);
show partitions table2;
alter table table2 add partition par_name3 values less than (9);
show partitions table2;
alter table table2 drop partition (par_name3);
show partitions table2;

drop table table1;
drop table table2;
