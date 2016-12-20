DROP TABLE datatest;

CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

FROM datatest
SELECT int_data1 , int_data2, boolean_data, double_data, string_data, SUM(boolean_data) OVER(partition by double_data);

DROP TABLE dataset;