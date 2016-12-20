drop table tst;
create table tst(a string, b string, d string) partition by list(d)
(partition p0 values in ('2008-12-31'));
alter table tst add partition p1 values in ('2009-01-01');

insert overwrite table tst
select src1.value, src2.value, '2009-01-01' from src src1 join src src2 ON (src1.key = src2.key and src1.key > 200);

select * from tst where tst.d='2009-01-01';

drop table tst;
