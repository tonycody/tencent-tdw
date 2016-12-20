set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

SELECT
  trunc(3.1415926),
  trunc(3.1415926,3),
  trunc(233.1415926,-1),
  trunc(3.14,null),
  trunc(null,null),
  trunc("2010-1-1 10:12:13",null),
  trunc("2010-12-24 11:05:22"),
  trunc("2010-12-24 11:05:22","YY"),
  trunc("2010-12-24 11:05:22","MM"),
  trunc("2010-12-27 11:05:22","WW"),
  trunc("2010-12-24 11:05:22","W"),
  trunc("2010-12-27 11:05:22","DAY"),
  trunc("2010-12-24 11:05:22","DDD"),
  trunc("2010-12-24 11:05:22","HH"),
  trunc("2010-12-24 11:05:22","MI")
FROM src LIMIT 1; 

CREATE TABLE test_trunc_num(td DOUBLE, tac INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_num_trunc' OVERWRITE INTO TABLE test_trunc_num; 

SELECT * 
FROM test_trunc_num;

SELECT 
  trunc(test_trunc_num.td,test_trunc_num.tac)
FROM test_trunc_num;

DROP TABLE test_trunc_num; 

CREATE TABLE test_trunc_date(tdate STRING, tac STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_date_trunc' OVERWRITE INTO TABLE test_trunc_date; 

SELECT * 
FROM test_trunc_date;

SELECT 
  trunc(test_trunc_date.tdate,test_trunc_date.tac)
FROM test_trunc_date;

DROP TABLE test_trunc_date; 
