DROP TABLE datatest;
DROP TABLE nulltest;
set analysisbuffer.tmp.addr=/tmp;
CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/datatest.txt' INTO TABLE datatest;
LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

explain select int_data1, max(f_act) over(partition by int_data1) as f_act,money from(select int_data1, 1 as f_act , cast(0 as double) as money  from datatest)m;

DROP TABLE datatest;
DROP TABLE nulltest;
