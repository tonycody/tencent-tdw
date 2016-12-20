DROP TABLE nulltest;
CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

SELECT COUNT(*) FROM (SELECT j.* FROM nulltest k left join nulltest j on (k.int_data2 = j.int_data1)) tmp;

SELECT COUNT(*) FROM (SELECT /*+mapjoin(j)*/ j.* FROM nulltest k left join nulltest j on (k.int_data2 = j.int_data1)) tmp;

SELECT COUNT(*) FROM (SELECT k.* FROM nulltest k right join nulltest j on (k.int_data1 = j.int_data2)) tmp;

SELECT COUNT(*) FROM (SELECT /*+mapjoin(k)*/ k.* FROM nulltest k right join nulltest j on (k.int_data1 = j.int_data2)) tmp;

DROP TABLE nulltest;