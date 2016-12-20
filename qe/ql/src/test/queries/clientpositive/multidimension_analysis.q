DROP TABLE datatest;

CREATE TABLE datatest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/datatest.txt' INTO TABLE datatest;

select sum(double_data), grouping(INT_DATA1*2+1), 10*(int_data2+5),grouping(int_data2), grouping(string_data), grouping(boolean_data),boolean_data from datatest group by CUBE(int_data2,string_data),ROLLUP(int_data1*2+1),CUBE(GROUP(boolean_data,string_data));

select * from (select sum(int_data1) s, case when grouping(int_data2) = 1 then 'all' else cast(int_data2 as string) end b from datatest group by CUBE(int_data2) order by s union all select sum(int_data1), 'the other' from datatest group by int_data2)unio order by s;

select * from (select sum(int_data1) s , int_data2,boolean_data,double_data from datatest group by CUBE(int_data2,boolean_data),ROLLUP(double_data) having s=1 order by s union all select sum(int_data1), null,null,null from datatest group by int_data2)unio order by s;

select count(distinct int_data1) from datatest where boolean_data = true group by rollup(boolean_data);

DROP TABLE datatest;
