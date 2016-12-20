DROP TABLE datatest;
set analysisbuffer.tmp.addr=/tmp;
CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/datatest.txt' INTO TABLE datatest;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(boolean_data) OVER(partition by int_data1) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(distinct boolean_data) OVER(partition by int_data1) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(string_data) OVER(partition by boolean_data) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(distinct string_data) OVER(partition by boolean_data) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(int_data2) OVER(partition by boolean_data) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(distinct int_data1) OVER(partition by boolean_data) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(double_data) OVER(partition by int_data1) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(distinct double_data) OVER(partition by int_data1) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(1) OVER(partition by int_data1) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(1) OVER(partition by boolean_data) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, COUNT(boolean_data) OVER(partition by int_data1,int_data2) FROM datatest) tmp ORDER BY double_data;

DROP TABLE datatest;