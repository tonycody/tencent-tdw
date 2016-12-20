DROP TABLE datatest;
set analysisbuffer.tmp.addr=/tmp;
CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/datatest.txt' INTO TABLE datatest;

SELECT * FROM (SELECT int_data1, int_data2, boolean_data, double_data, string_data, LEAD(boolean_data, 1) OVER(partition by int_data1 order by int_data2) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, LEAD(double_data, 1) OVER(partition by int_data1 order by int_data2) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, LEAD(string_data, 2) OVER(partition by int_data1 order by int_data2) FROM datatest) tmp ORDER BY double_data;

DROP TABLE datatest;