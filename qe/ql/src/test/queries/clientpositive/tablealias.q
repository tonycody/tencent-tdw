set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;
set analysisbuffer.tmp.addr=/tmp;

DROP TABLE nulltest;

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltest;

select n.int_data1, n.int_data2, sum(1) as play_cnt from nulltest n group by int_data1, int_data2;

select nulltest.int_data1, nulltest.int_data2, sum(1) as play_cnt from nulltest n group by int_data1, int_data2;

select n.int_data1, n.int_data2, sum(1) as play_cnt from nulltest n group by n.int_data1, n.int_data2;

select int_data1, int_data2, sum(1) as play_cnt from nulltest n group by n.int_data1, n.int_data2;

select int_data1, int_data2, sum(1) as play_cnt from nulltest n group by int_data1, int_data2;

select dt.int_data1, dt.int_data2,dt.play_cnt from (select n.int_data1, n.int_data2, sum(1) as play_cnt from nulltest n group by n.int_data1, n.int_data2) dt;

select dt.int_data1, dt.int_data2,dt.play_cnt, row_number() over(partition by dt.int_data1 order by dt.play_cnt desc) as ranking from (select n.int_data1, n.int_data2, sum(1) as play_cnt from nulltest n group by n.int_data1, n.int_data2) dt order by dt.int_data1, dt.int_data2;

select dt.int_data1, dt.int_data2,dt.play_cnt, row_number() over(partition by int_data1 order by play_cnt desc) as ranking from (select n.int_data1, n.int_data2, sum(1) as play_cnt from nulltest n group by n.int_data1, n.int_data2) dt order by dt.int_data1, dt.int_data2;


DROP TABLE nulltest;
