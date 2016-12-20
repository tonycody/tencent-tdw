EXPLAIN FROM (  FROM src select src.* WHERE src.key < 100) unioninput INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/union.out' SELECT unioninput.*;

drop table union_tmp;

create table union_tmp as select * FROM (  FROM src select src.* WHERE src.key < 100) unioninput;

select * from union_tmp order by key desc;

drop table union_tmp;
