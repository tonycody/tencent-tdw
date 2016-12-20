DROP TABLE tsttbl;
CREATE TABLE tsttbl(KEY int, VALUE STRING) partition by range(KEY)(
partition p1 values less than(99999)
);

from src
insert overwrite table tsttbl
select *;

DROP TABLE tsttbl2;
CREATE TABLE tsttbl2(KEY int, VALUE STRING);
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE tsttbl2;

explain
select u.* from
(
  select key, value from tsttbl x where x.key > 99999
    union all  
  select key, value from tsttbl2 y
)u;

select u.* from
(
  select key, value from tsttbl x where x.key > 99999
    union all  
  select key, value from tsttbl2 y
)u;


DROP TABLE tsttbl;
DROP TABLE tsttbl2;
