set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

DROP TABLE nulltest;

CREATE TABLE nulltest(int_data1 INT, int_data2 BIGINT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

SELECT * FROM nulltest WHERE int_data2 in (5, null) and boolean_data not in (true, null);

SELECT * FROM nulltest WHERE int_data2 not in (0, 1, CAST(2 AS BIGINT)) and double_data not in (16.91358120533963, NULL);

SELECT * FROM nulltest WHERE boolean_data in (false, NULL) and string_data in ('The', null);

SELECT * FROM nulltest WHERE boolean_data not in (true, null) and string_data not in ('TDW', null);

SELECT * FROM nulltest WHERE double_data in (16.91358120533963, 32.92404678679056, null) and int_data2 in (5, null);

SELECT * FROM nulltest WHERE double_data not in (NULL) and boolean_data not in (true, null);

SELECT * FROM nulltest WHERE string_data in ('The', null) and  int_data2 in (5, null) ;

SELECT * FROM nulltest WHERE string_data not in ('TDW', null) and boolean_data not in (true, null);

drop table nulltest;