drop table alter3_src;
drop table alter3;
drop table alter3_renamed;
set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;


create table alter3_src ( col1 string ) stored as textfile ;
load data local inpath '../data/files/test.dat' overwrite into table alter3_src ;

create table alter3 ( col1 string, pcol1 string, pcol2 string) partition by list(pcol1) subpartition by list(pcol2) (subpartition sp1 values in ("test_part")) (partition p1 values in ("test_part")) stored as sequencefile;

insert overwrite table alter3 select col1, "test_part", "test_part" from alter3_src ;
select * from alter3 where pcol1="test_part" and pcol2="test_part";

alter table alter3 rename to alter3_renamed;
describe extended alter3_renamed;
select * from alter3_renamed where pcol1='test_part' and pcol2='test_part';

drop table alter3_src;
drop table alter3;
drop table alter3_renamed;
