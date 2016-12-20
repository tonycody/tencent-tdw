CREATE TABLE test_chr (tchr INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_chr' OVERWRITE INTO TABLE test_chr; 

SELECT * 
FROM test_chr;

SELECT 
  chr(test_chr.tchr)
FROM test_chr;

DROP TABLE test_chr;   
