-- Can't update view to have a view cycle
drop view v;
create view v1 as select * from srcpart;

create or replace view v1 as select * from v1;