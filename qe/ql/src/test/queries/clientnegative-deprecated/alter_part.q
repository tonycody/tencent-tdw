set hive.cli.errors.ignore=true;
--
drop table tstparttbl;
create table tstparttbl(a int, b int,ds string) partition by list(ds)(partition p1 values in('x','y'),
partition p2 values in ("a","b"),
partition default
);
alter table tstparttbl add partition p3 values less than('zz');
show partitions tstparttbl;
