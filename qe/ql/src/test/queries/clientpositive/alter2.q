drop table alter2;
set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

create table alter2(a int, b int, insertdate int) partition by list(insertdate) (partition p0 values in (20080101));
describe extended alter2;
show partitions alter2;
alter table alter2 add partition p1 values in (20080102); 
describe extended alter2;
show partitions alter2;
alter table alter2 add partition p2 values in (20080103);
describe extended alter2;
show partitions alter2;
drop table alter2;
