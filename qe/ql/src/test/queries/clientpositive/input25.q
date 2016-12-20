drop table tst;
create table tst(a int, b int, d string) partition by list(d)
(partition p0 values in ('2008-12-31'));
alter table tst add partition p1 values in ('2009-01-01');
alter table tst add partition p2 values in ('2009-02-02');

explain
select * from (
  select * from tst x where x.d='2009-01-01' limit 10
    union all
  select * from tst x where x.d='2009-02-02' limit 10
) subq;

select * from (
  select * from tst x where x.d='2009-01-01' limit 10
    union all
  select * from tst x where x.d='2009-02-02' limit 10
) subq;

drop table tst;
