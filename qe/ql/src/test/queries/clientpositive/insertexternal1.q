drop table texternal;

create table texternal(key string, val string, insertdate string)
partition by list(insertdate) (partition p0 values in ('2008-00-01'));

alter table texternal add partition p1 values in ('2008-01-01');
from src insert overwrite table texternal select key, value, '2008-01-01';

select * from texternal where insertdate='2008-01-01';