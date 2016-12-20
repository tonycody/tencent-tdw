DROP TABLE datatest;
set analysisbuffer.tmp.addr=/tmp;
CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/datatest.txt' INTO TABLE datatest;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, LAG(string_data, 1, NULL) OVER(partition by int_data1 order by int_data2) FROM datatest) tmp ORDER BY double_data;

SELECT * FROM (SELECT int_data1 , int_data2, boolean_data, double_data, string_data, LAG(double_data, 1, NULL) OVER(partition by int_data1 order by int_data2) FROM datatest) tmp ORDER BY double_data;

DROP TABLE datatest;