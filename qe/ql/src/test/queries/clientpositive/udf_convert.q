set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

CREATE TABLE test_convert (str STRING, des STRING, src STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_convert' OVERWRITE INTO TABLE test_convert; 

SELECT * 
FROM test_convert;

SELECT 
  convert(test_convert.str,test_convert.des, test_convert.src)
FROM test_convert;

DROP TABLE test_convert;   
