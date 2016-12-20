set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

DROP TABLE nulltest;
DROP TABLE nulltest1;

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

CREATE TABLE nulltest1(int_data1 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT TABLE nulltest1 SELECT int_data1 FROM nulltest;

EXPLAIN SELECT int_data2,COUNT(1) FROM (SELECT /*+MAPJOIN(A)*/ int_data2, 1 AS C1, 0 AS C2 FROM nulltest A JOIN nulltest1 B ON(A.INT_DATA1 = B.INT_DATA1) UNION ALL SELECT /*+MAPJOIN(A)*/ INT_DATA2, 1 AS C1, 2 AS C2 FROM nulltest A JOIN nulltest1 B ON(A.INT_DATA1 = B.INT_DATA1)) MAPJOINTABLE GROUP BY int_data2;

SELECT int_data2,COUNT(1) FROM (SELECT /*+MAPJOIN(A)*/ int_data2, 1 AS C1, 0 AS C2 FROM nulltest A JOIN nulltest1 B ON(A.INT_DATA1 = B.INT_DATA1) UNION ALL SELECT /*+MAPJOIN(A)*/ INT_DATA2, 1 AS C1, 2 AS C2 FROM nulltest A JOIN nulltest1 B ON(A.INT_DATA1 = B.INT_DATA1)) MAPJOINTABLE GROUP BY int_data2;

SELECT int_data2,COUNT(1) FROM (SELECT int_data2, 1 AS C1, 0 AS C2 FROM nulltest A JOIN nulltest1 B ON(A.INT_DATA1 = B.INT_DATA1) UNION ALL SELECT INT_DATA2, 1 AS C1, 2 AS C2 FROM nulltest A JOIN nulltest1 B ON(A.INT_DATA1 = B.INT_DATA1)) MAPJOINTABLE GROUP BY int_data2;

DROP TABLE nulltest;
DROP TABLE nulltest1;
