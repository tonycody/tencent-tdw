drop table person;
create table person stored as pb;
desc extended person;
explain insert into person(name,id,email) values('allison',28,'xx@qq.com'),('paynie',25,'fsd@qq.com');

explain select name,id,email,phone from person order by id;

explain insert table person select name,id,email,array('13688888','18688888') from person;

explain select name,phone[1] from person where id = 25 order by name;
explain select name,phone[1] from person where id = 28 order by name;

drop table person;

