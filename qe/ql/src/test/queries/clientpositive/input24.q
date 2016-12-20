drop table tst;
create table tst(a int, b int, d string) partition by list(d)
(partition p0 values in ('2008-12-31'));
alter table tst add partition p1 values in ('2009-01-01');
explain
select count(1) from tst x where x.d='2009-01-01';

select count(1) from tst x where x.d='2009-01-01';

drop table tst;
