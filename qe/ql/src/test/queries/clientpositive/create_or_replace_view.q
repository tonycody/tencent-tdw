drop view v;

create view v as select * from srcpart;
describe extended v;
create or replace view v as select ds, hr from srcpart;

describe extended v;

-- altering partitioned view 1
create or replace view v as select value, ds, hr from srcpart;
select * from v where value='val_409' and ds='2008-04-08' and hr='11';

describe extended v;

drop view v;

-- updating to fix view with invalid definition

create table srcpart_temp like srcpart;

create view v as select * from srcpart_temp;
drop table srcpart_temp; -- v is now invalid

create or replace view v as select * from srcpart;
describe extended v;

drop view v;