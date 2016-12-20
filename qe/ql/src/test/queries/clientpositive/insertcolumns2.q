drop table insert1;
set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;


create table insert1(key int, value string) stored as textfile;


explain 
insert overwrite table insert1(value, key) select * from src where key > 10;

insert overwrite table insert1(value, key) select * from src where key > 10;

select * from insert1;

drop table insert1;

