
-- to guarantee that a sql which contains both 'join' and 'where' subclause, 
-- the one side of the join contains a table and the other side contains a sub query
-- in such circumstances the partition prune may give a error, this q is to check that
 
create table jswp (a int, b int);
create table jswp1 like jswp;
create table jswp2 like jswp;

explain insert overwrite table jswp2 select t2.a, t2.b from (select a, count(1) as num from jswp group by a) t1 join jswp1 t2 on(t1.a = t2.a) where t1.a>4;

drop table jswp;
drop table jswp1;
drop table jswp2;


