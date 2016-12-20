DROP TABLE nulltest;

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

SELECT sum(int_data1),int_data2,boolean_data from nulltest where int_data2 in(1,2) and boolean_data in (true, false) group by int_data2,boolean_data;

SELECT sum(int_data1),int_data2, boolean_data from nulltest where boolean_data not in (null) group by int_data2,boolean_data having sum(int_data1) not in(cast(0 as bigint));

DROP TABLE nulltest;