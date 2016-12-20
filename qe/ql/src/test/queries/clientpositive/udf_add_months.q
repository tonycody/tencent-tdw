CREATE TABLE test_add_months (tdate STRING , tn INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_add_months' OVERWRITE INTO TABLE test_add_months;

SELECT * 
FROM test_add_months;

SELECT 
  add_months(test_add_months.tdate,test_add_months.tn)
FROM test_add_months;  

DROP TABLE test_add_months;
