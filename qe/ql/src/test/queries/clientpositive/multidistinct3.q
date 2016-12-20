
DROP TABLE srcmd;
CREATE TABLE srcmd(key STRING, value STRING, ds STRING, hr STRING);
LOAD DATA LOCAL INPATH '../data/files/kv6.txt' INTO TABLE srcmd;

DROP TABLE dest_mt3;
CREATE TABLE dest_mt3(hr STRING, keysum INT, keynum INT, cnt INT, dsnum INT) STORED AS TEXTFILE;

EXPLAIN
FROM srcmd
INSERT OVERWRITE TABLE dest_mt3 SELECT srcmd.hr, sum(srcmd.key), count(distinct srcmd.key), count(1), count(distinct srcmd.ds) group by srcmd.hr;

FROM srcmd
INSERT OVERWRITE TABLE dest_mt3 SELECT srcmd.hr, sum(srcmd.key), count(distinct srcmd.key), count(1), count(distinct srcmd.ds) group by srcmd.hr;

SELECT dest_mt3.* FROM dest_mt3;

DROP TABLE dest_mt3;
DROP TABLE srcmd;
