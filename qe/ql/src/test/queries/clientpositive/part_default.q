DROP TABLE nulltest;
DROP TABLE nulltest2;

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

CREATE TABLE nulltest2(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) PARTITION BY LIST(int_data1) (partition par_name1 values in (0), partition par_name2 values in (1),PARTITION DEFAULT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT TABLE nulltest2 SELECT * FROM nulltest;

EXPLAIN SELECT * FROM nulltest2 WHERE int_data1 = 1 or int_data1 = 2;

SELECT * FROM nulltest2 WHERE int_data1 = 1 or int_data1 = 2 order by int_data1,int_data2,double_data;

DROP TABLE nulltest;
DROP TABLE nulltest2; 

