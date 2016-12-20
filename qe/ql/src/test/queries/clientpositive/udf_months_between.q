CREATE TABLE test_months_between(tdate1 STRING,tdate2 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_months_between' OVERWRITE INTO TABLE test_months_between; 

SELECT * 
FROM test_months_between;

SELECT 
  months_between(test_months_between.tdate1,test_months_between.tdate2)
FROM test_months_between;

DROP TABLE test_months_between;   
