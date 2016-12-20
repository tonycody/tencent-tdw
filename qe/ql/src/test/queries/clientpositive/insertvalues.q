set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;


drop table insert1;

create table insert1(a tinyint, b smallint, c int, d bigint, e float, f double, g string);

insert into insert1(a) values(1),('-1'),(1000),('-1000'),(NULL),('k'),(1.2);

insert into insert1(b) values(1),('-1'),(1000000),('-1000000'),(NULL),('k'),(1.2);

insert into insert1(c) values(1),('-1'),(5000000000),('-5000000000'),(NULL),('k'),(1.2);

insert into insert1(d) values(1),('-1'),(NULL),('k'),(1.2);

insert into insert1(e) values(1),('-1'),(5000000000),('-5000000000'),(NULL),('k'),(1.2),('1.2e100'),('-1.2e-100'),('-1.2e-10');

insert into insert1(f) values(1),('-1'),(5000000000),('-5000000000'),(NULL),('k'),(1.2),(1.43258745652145744),('1.2e100000'),('-1.2e-100000'),('-1.2e-100');

insert into insert1(g) values(1),('-1'),(5000000000),('-5000000000'),(NULL),('k'),(1.2),(1.43258745652145744),('1.2e100000'),('-1.2e-100000'),('-1.2e-100');

insert into insert1(a,b,c,d) values(1,2,3,4),(5,6,7,8);

insert into insert1(e,f,g) values(1.0,1.2,1.3),(1.4,1.5,'test');

insert into insert1 values(1,2,3,4,5,6,'test');

select * from insert1 order by a,b,c,d,e,f,g;

drop table insert1;

