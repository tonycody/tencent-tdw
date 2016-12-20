
DROP TABLE srcmd;
CREATE TABLE srcmd(key STRING, value STRING, ds STRING, hr STRING);
LOAD DATA LOCAL INPATH '../data/files/kv6.txt' INTO TABLE srcmd;

DROP TABLE dest_mt2;
CREATE TABLE dest_mt2(cnt INT, keysum INT, keynum INT, dsnum INT) STORED AS TEXTFILE;

EXPLAIN
FROM srcmd
INSERT OVERWRITE TABLE dest_mt2 SELECT count(1), sum(srcmd.key), count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

FROM srcmd
INSERT OVERWRITE TABLE dest_mt2 SELECT count(1), sum(srcmd.key), count(distinct srcmd.key), count(distinct srcmd.ds) group by srcmd.hr;

SELECT dest_mt2.* FROM dest_mt2;

DROP TABLE dest_mt2;
DROP TABLE srcmd;
