set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

SELECT 
  sign(1),
  sign(-1),
  sign(0),
  sign(-0),
  sign(0.0000000000001),
  sign(-0.0000000000001),
  sign(0.000),
  sign(-0.000),
  sign(15.227),
  sign(-456.328),
  sign(888888888888855555523238),
  sign(-124444444444444444444444)
FROM src LIMIT 1; 

CREATE TABLE test_sign_int(tint INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_int_sign' OVERWRITE INTO TABLE test_sign_int; 

SELECT * 
FROM test_sign_int;

SELECT 
  sign(test_sign_int.tint)
FROM test_sign_int;

DROP TABLE test_sign_int; 

CREATE TABLE test_sign_double(td DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_double_sign' OVERWRITE INTO TABLE test_sign_double; 

SELECT * 
FROM test_sign_double;

SELECT 
  sign(test_sign_double.td)
FROM test_sign_double;

DROP TABLE test_sign_double; 

