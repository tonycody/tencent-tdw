
DROP TABLE srcmd;
CREATE TABLE srcmd(key STRING, value STRING, ds STRING, hr STRING);
LOAD DATA LOCAL INPATH '../data/files/kv6.txt' INTO TABLE srcmd;

DROP TABLE dest_mt1;
CREATE TABLE dest_mt1(hr STRING, keynum INT, dsnum INT) STORED AS TEXTFILE;

set hive.map.aggr=false;
set hive.groupby.skewindata=false;

EXPLAIN FROM srcmd INSERT OVERWRITE TABLE dest_mt1 SELECT srcmd.hr, count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

FROM srcmd INSERT OVERWRITE TABLE dest_mt1 SELECT srcmd.hr, count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

SELECT dest_mt1.* FROM dest_mt1;

set hive.map.aggr=true;
set hive.groupby.skewindata=false;

EXPLAIN FROM srcmd INSERT OVERWRITE TABLE dest_mt1 SELECT srcmd.hr, count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

FROM srcmd INSERT OVERWRITE TABLE dest_mt1 SELECT srcmd.hr, count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

SELECT dest_mt1.* FROM dest_mt1;

DROP TABLE dest_mt1;
CREATE TABLE dest_mt1(hr STRING, keynum INT, dsnum INT) STORED AS TEXTFILE;

set hive.map.aggr=false;
set hive.groupby.skewindata=true;

EXPLAIN FROM srcmd INSERT OVERWRITE TABLE dest_mt1 SELECT srcmd.hr, count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

FROM srcmd INSERT OVERWRITE TABLE dest_mt1 SELECT srcmd.hr, count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

SELECT dest_mt1.* FROM dest_mt1;

set hive.map.aggr=true;
set hive.groupby.skewindata=true;

EXPLAIN FROM srcmd INSERT OVERWRITE TABLE dest_mt1 SELECT srcmd.hr, count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

FROM srcmd INSERT OVERWRITE TABLE dest_mt1 SELECT srcmd.hr, count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

SELECT dest_mt1.* FROM dest_mt1;

DROP TABLE dest_mt1;
DROP TABLE srcmd;
