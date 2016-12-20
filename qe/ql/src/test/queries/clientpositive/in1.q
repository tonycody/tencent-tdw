set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

DROP TABLE nulltest;

CREATE TABLE nulltest(int_data1 INT, int_data2 BIGINT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

SELECT * FROM nulltest WHERE int_data2 in (5, null);

SELECT * FROM nulltest WHERE int_data2 not in (0, 1, 2);

SELECT * FROM nulltest WHERE int_data1 in(cast(5 as bigint), 4);

SELECT * FROM nulltest WHERE int_data1 not in (cast(0 as bigint), cast(1 as bigint));

SELECT * FROM nulltest WHERE boolean_data in (false, NULL);

SELECT * FROM nulltest WHERE boolean_data not in (true, null);

SELECT * FROM nulltest WHERE double_data in (16.91358120533963, 32.92404678679056, null);

SELECT * FROM nulltest WHERE double_data not in (NULL);

SELECT * FROM nulltest WHERE string_data in ('The', null);

SELECT * FROM nulltest WHERE string_data not in ('TDW', null);

drop table nulltest;