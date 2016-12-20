create table testtable like srcpart;

insert overwrite table testtable select * from srcpart;

alter table testtable truncate partition(p0);

select * from testtable partition(p0) x;

drop table testtable;
