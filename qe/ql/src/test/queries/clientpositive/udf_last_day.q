CREATE TABLE test_last_day (tdate STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_last_day' OVERWRITE INTO TABLE test_last_day; 

SELECT * 
FROM test_last_day;

SELECT 
  last_day(test_last_day.tdate)
FROM test_last_day;

DROP TABLE test_last_day;   
