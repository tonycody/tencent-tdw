drop table insert1;

create table insert1(key int, value string) stored as textfile;


explain 
insert overwrite table insert1(value, key) select value, key from src where key > 10;

insert overwrite table insert1(value, key) select value, key from src where key > 10;

select * from insert1;

drop table insert1;

