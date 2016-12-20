set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

SELECT
  least(5,2,null,8),
  least(null,1,8,456),
  least(1,2,5,3,null),
  least(42,12,-34,6,-12),
  least(1.2,5.8,98.1,-0.001),
  least(23.4,-12.6,null),
  least("ffsf","adfdf","asd","adfde","adf"),
  least("ffsf","adfdf","asd",null,"adfde","adf"),
  least("ffsf","adfdf","asd","adfde")
FROM src LIMIT 1; 


CREATE TABLE test_least_string(tstr_1 STRING, tstr_2 STRING, tstr_3 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_least_string' OVERWRITE INTO TABLE test_least_string; 

select
  least(test_least_string.tstr_1,test_least_string.tstr_2,test_least_string.tstr_3),test_least_string.tstr_1,test_least_string.tstr_2,test_least_string.tstr_3
from test_least_string order by test_least_string.tstr_1;

DROP TABLE test_least_string; 

CREATE TABLE test_least_int(tstr_1 INT, tstr_2 INT, tstr_3 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_least_int' OVERWRITE INTO TABLE test_least_int; 

select
  least(test_least_int.tstr_1,test_least_int.tstr_2,test_least_int.tstr_3),test_least_int.tstr_1,test_least_int.tstr_2,test_least_int.tstr_3
from test_least_int order by test_least_int.tstr_1;

DROP TABLE test_least_int; 

CREATE TABLE test_least_double(tstr_1 DOUBLE, tstr_2 DOUBLE, tstr_3 DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_least_double' OVERWRITE INTO TABLE test_least_double; 

select
  least(test_least_double.tstr_1,test_least_double.tstr_2,test_least_double.tstr_3),test_least_double.tstr_1,test_least_double.tstr_2,test_least_double.tstr_3
from test_least_double order by test_least_double.tstr_1, test_least_double.tstr_2;

DROP TABLE test_least_double; 
