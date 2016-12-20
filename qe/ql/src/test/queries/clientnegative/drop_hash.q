create table table1;
create table table1 (col_name1 int, col_name2 int) partition by hashkey(col_name2);
alter table table1 drop partition (Hash_0006);
drop table table1;
