drop table insert1;

create table insert1(key int, numvalue int) stored as textfile;


explain 
insert overwrite table insert1(key, numvalue) select count(value), count(distinct value) from src where key > 10 group by key;

insert overwrite table insert1(key, numvalue) select count(value), count(distinct value) from src where key > 10 group by key;

select * from insert1;

drop table insert1;

