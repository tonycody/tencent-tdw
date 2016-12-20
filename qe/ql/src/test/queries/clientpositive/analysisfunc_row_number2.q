DROP TABLE nulltest;
set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;
set analysisbuffer.tmp.addr=/tmp;

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, ROW_NUMBER() OVER(partition by boolean_data order by double_data) FROM nulltest) tmp ORDER BY double_data, string_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, ROW_NUMBER() OVER(partition by boolean_data order by string_data desc) FROM nulltest) tmp ORDER BY double_data, string_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, ROW_NUMBER() OVER(partition by boolean_data order by int_data1 desc) FROM nulltest) tmp ORDER BY double_data, string_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, ROW_NUMBER() OVER(partition by string_data order by boolean_data) FROM nulltest) tmp ORDER BY double_data, string_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, ROW_NUMBER() OVER(partition by double_data order by boolean_data) FROM nulltest) tmp ORDER BY double_data, string_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, ROW_NUMBER() OVER(partition by int_data1 order by boolean_data) FROM nulltest) tmp ORDER BY double_data, string_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, ROW_NUMBER() OVER(partition by int_data1,int_data2 order by boolean_data) FROM nulltest) tmp ORDER BY double_data, string_data;

DROP TABLE nulltest;