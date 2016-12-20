SELECT
  decode(5,2,21,8),
  decode(1,1,"goog",456,null,null),
  decode(1,2,5.3,3,4.22,2.314),
  decode(1.2,1.20002,5656,1.2,56,1.2,23,123),
  decode(1.2,5.8,98.1,-0.001),
  decode(23.4,-23.4,444,555),
  decode("a","b","fasf","c","adf",null),
  decode("one","one",123,"",555,888),
  decode("ffsf","adfdf",1.1,"asd",2.2,3.3)
FROM src LIMIT 1; 

drop table test_decode_1;
CREATE TABLE test_decode_1(fir INT , tint_1 INT ,tstr_1 STRING, tint_2 INT , tstr_2 STRING, tint_3 INT , tstr_3 STRING, def  STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_decode_1_data' OVERWRITE INTO TABLE test_decode_1; 

select
decode(fir,tint_1,tstr_1,tint_2,tstr_2,tint_3, tstr_3,def  )
from test_decode_1;

DROP TABLE test_decode_1; 
