DROP TABLE nulltest;
CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;
SELECT /*+MAPJOIN(j)*/ * FROM nulltest j JOIN nulltest k ON (j.int_data2 = k.int_data2 and j.int_data1 = 1);
SELECT * FROM nulltest j JOIN nulltest k ON (j.int_data2 = k.int_data2 and j.int_data1 = 1);
DROP TABLE nulltest;
