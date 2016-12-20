DROP TABLE testtbl;
CREATE TABLE testtbl(KEY INT, VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES("charset"="gbk");
LOAD DATA LOCAL INPATH '../data/files/kv.txt' INTO TABLE testtbl;
SELECT COUNT(*) FROM testtbl;
