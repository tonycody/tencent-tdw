
DROP TABLE srcmd;
CREATE TABLE srcmd(key STRING, value STRING, ds STRING, hr STRING);
LOAD DATA LOCAL INPATH '../data/files/kv6.txt' INTO TABLE srcmd;

DROP TABLE dest_mt5;
CREATE TABLE dest_mt5(keycnt INT, valuecnt INT) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_mt5 SELECT count(distinct src.key), count(distinct src.value);

FROM src
INSERT OVERWRITE TABLE dest_mt5 SELECT count(distinct src.key), count(distinct src.value);

SELECT dest_mt5.* FROM dest_mt5;

DROP TABLE dest_mt5;
DROP TABLE srcmd;
