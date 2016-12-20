set hive.map.aggr=false;
set hive.groupby.skewindata=true;

drop table dest1;
drop table dest2;

CREATE TABLE dest1(key STRING, val1 INT, val2 INT, ds string) partition by list(ds) (partition p0 values in ('111'));
CREATE TABLE dest2(key STRING, val1 INT, val2 INT, ds string) partition by list(ds) (partition p0 values in ('111'));

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1
  SELECT src.value, count(src.key), count(distinct src.key), '111' GROUP BY src.value
INSERT OVERWRITE TABLE dest2
  SELECT substr(src.value, 5), count(src.key), count(distinct src.key), '111' GROUP BY substr(src.value, 5);

FROM src
INSERT OVERWRITE TABLE dest1
  SELECT src.value, count(src.key), count(distinct src.key), '111' GROUP BY src.value
INSERT OVERWRITE TABLE dest2
  SELECT substr(src.value, 5), count(src.key), count(distinct src.key), '111' GROUP BY substr(src.value, 5);

SELECT * from dest1;
SELECT * from dest2;

drop table dest1;
drop table dest2;
