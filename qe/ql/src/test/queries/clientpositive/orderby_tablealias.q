DROP TABLE datatest;

CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/datatest.txt' INTO TABLE datatest;

SELECT datatest.int_data1 FROM datatest GROUP BY datatest.int_data1 ORDER BY datatest.int_data1;

DROP TABLE datatest;
