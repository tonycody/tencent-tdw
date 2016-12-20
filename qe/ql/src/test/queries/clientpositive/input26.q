explain
select * from (   select * from srcpart a where a.ds = '2008-04-08' and a.hr = '09' order by a.key limit 5     union all  select * from srcpart b where b.ds = '2008-04-08' and b.hr = '21' limit 5)subq sort by subq.key;
--note: the test table srcpart.key is string type,so the sort result is string order!
select * from (  select * from srcpart a where a.ds = '2008-04-08' and a.hr = '09' order by a.key limit 5    union all  select * from srcpart b where b.ds = '2008-04-08' and b.hr = '21' limit 5)subq sort by subq.key;
