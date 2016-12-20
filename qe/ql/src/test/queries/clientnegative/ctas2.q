drop table nulltest1;

CREATE TABLE nulltest1(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table tmp100 stored as pbfile as select * from (select * from nulltest1 a join nulltest1 b on(a.int_data1=b.int_data2))dt;

drop table nulltest1;
