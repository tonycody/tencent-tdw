DROP TABLE datatest;
DROP TABLE nulltest;

CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/datatest.txt' INTO TABLE datatest;

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

explain SELECT * FROM datatest t1 WHERE EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2 )  and not EXISTS(SELECT t3.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data, string_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 3) t3 WHERE t1.int_data2=t3.int_data2) order by int_data2;

SELECT * FROM datatest t1 WHERE EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2 )  and not EXISTS(SELECT t3.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data, string_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 3) t3 WHERE t1.int_data2=t3.int_data2) order by int_data2;

explain SELECT * FROM datatest t1 WHERE EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2 )  and EXISTS(SELECT t3.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data, string_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 3) t3 WHERE t1.int_data2=t3.int_data2) order by int_data2;

SELECT * FROM datatest t1 WHERE EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2 )  and EXISTS(SELECT t3.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data, string_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 3) t3 WHERE t1.int_data2=t3.int_data2) order by int_data2;

explain SELECT * FROM datatest t1 WHERE NOT EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2 )  and not EXISTS(SELECT t3.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data, string_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 3) t3 WHERE t1.int_data2=t3.int_data2) order by int_data2;

SELECT * FROM datatest t1 WHERE NOT EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2 )  and not EXISTS(SELECT t3.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data, string_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 3) t3 WHERE t1.int_data2=t3.int_data2) order by int_data2, double_data;

explain SELECT * FROM datatest t1 WHERE not EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2 )  and EXISTS(SELECT t3.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data, string_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 3) t3 WHERE t1.int_data2=t3.int_data2) order by int_data2;

SELECT * FROM datatest t1 WHERE not EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2 )  and EXISTS(SELECT t3.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data, string_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 3) t3 WHERE t1.int_data2=t3.int_data2) order by int_data2, double_data;

DROP TABLE datatest;
DROP TABLE nulltest;
