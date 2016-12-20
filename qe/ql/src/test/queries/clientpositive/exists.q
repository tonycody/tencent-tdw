DROP TABLE datatest;
DROP TABLE nulltest;

CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/datatest.txt' INTO TABLE datatest;

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

EXPLAIN SELECT * FROM datatest t1 WHERE EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2)  order by int_data2;

SELECT * FROM datatest t1 WHERE EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest where boolean_data is null) t2 WHERE t1.int_data1=t2.int_data2)  order by int_data2;

EXPLAIN SELECT * FROM datatest t1 WHERE EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, double_data as dbl FROM nulltest where  string_data is null and not boolean_data is null) t2 WHERE t1.double_data=t2.dbl)  order by int_data2;

SELECT * FROM datatest t1 WHERE EXISTS(SELECT 1 FROM (SELECT int_data2 AS int_data2, double_data as dbl FROM nulltest where  string_data is null and not boolean_data is null) t2 WHERE t1.double_data=t2.dbl)  order by int_data2;

EXPLAIN SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 2) t2 WHERE t1.int_data1=t2.int_data2) AND (int_data2=1 OR (boolean_data=false AND int_data2=0) ) order by int_data2;

SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 2) t2 WHERE t1.int_data1=t2.int_data2) AND (int_data2=1 OR (boolean_data=false AND int_data2=0) ) order by int_data2;

EXPLAIN SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 2) t2 WHERE t1.int_data1=t2.int_data2) and (boolean_data=false or (int_data1=2 and int_data2=3))  order by int_data2;

SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 2) t2 WHERE t1.int_data1=t2.int_data2) and (boolean_data=false or (int_data1=2 and int_data2=3))  order by int_data2;

EXPLAIN select string_data, int_data2 from (SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 2) t2 WHERE t1.int_data1=t2.int_data2) and (boolean_data=false or (int_data1=2 and int_data2=3))  order by int_data2 limit 3) t3 order by string_data;

select string_data, int_data2 from (SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM (SELECT int_data2 AS int_data2, boolean_data FROM nulltest WHERE string_data IS NULL order by boolean_data limit 2) t2 WHERE t1.int_data1=t2.int_data2) and (boolean_data=false or (int_data1=2 and int_data2=3))  order by int_data2 limit 3) t3 order by string_data;

explain SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM nulltest t2 WHERE  t1.int_data1=t2.int_data2 and string_data IS NULL and not boolean_data is null) and (boolean_data=false or (int_data1=2 and int_data2=3) )  order by int_data2;

SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM nulltest t2 WHERE  t1.int_data1=t2.int_data2 and string_data IS NULL and not boolean_data is null) and (boolean_data=false or (int_data1=2 and int_data2=3) )  order by int_data2;


explain SELECT * FROM datatest t1 WHERE EXISTS(SELECT t2.int_data2 FROM nulltest t2 WHERE  t1.int_data1=t2.int_data2 and t1.int_data1= t2.int_data1 and string_data IS NULL and not boolean_data is null) and (boolean_data=false or (int_data1=2 and int_data2=3) )  order by int_data2;

explain SELECT int_data2,SUM(int_data1) FROM datatest t1 WHERE  EXISTS(SELECT t2.int_data2 FROM nulltest t2 WHERE string_data IS NULL AND t1.int_data2=t2.int_data2) AND (boolean_data=false OR (int_data1=2 AND int_data2=3)) GROUP BY int_data2;

SELECT int_data2,SUM(int_data1) FROM datatest t1 WHERE  EXISTS(SELECT t2.int_data2 FROM nulltest t2 WHERE string_data IS NULL AND t1.int_data2=t2.int_data2) AND (boolean_data=false OR (int_data1=2 AND int_data2=3)) GROUP BY int_data2;

DROP TABLE datatest;
DROP TABLE nulltest;
