DROP TABLE nulltest1;
DROP TABLE nulltest2;

CREATE TABLE nulltest1(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest1;

CREATE TABLE nulltest2 as SELECT int_data1,int_data2 FROM nulltest1;

SELECT 0,0,0,0 FROM nulltest1 a JOIN nulltest2 b ON(a.int_data1=1 AND b.int_data3=1 AND a.int_data1=b.int_data4 AND int_data3 >0);

DROP TABLE nulltest1;
DROP TABLE nulltest2;
