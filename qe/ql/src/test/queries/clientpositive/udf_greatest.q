create table regexp_instr_test(domain_id string);
LOAD DATA LOCAL INPATH '../data/files/domain_id.txt' OVERWRITE INTO TABLE regexp_instr_test;

select greatest('abc', 'abcd', 'abcde', 'abcdf', null)
,greatest('abc', 'abc') 
,greatest('abc', 'abcd')
,greatest('abc', 'abcd', 'abcde', 'abcdf')
,greatest(1, 1)
,greatest(1, 1000)
,greatest(1, 1000, null)
,greatest(cast(1 as smallint), 1000)
,greatest(6, 6.1)
,greatest(6, 5.9)
,greatest(cast (5 as tinyint), cast(6 as smallint), cast(7 as int), cast(8 as bigint))
,greatest(cast (5 as tinyint), cast(6 as smallint), cast(7 as int), cast(8 as bigint), 7.1)
,greatest(1, 2, least(3, 4, 5), 9)
,greatest(1, 2, greatest(3, 4, 5.0), 4) from regexp_instr_test limit 1;

drop table regexp_instr_test;


CREATE TABLE test_greatest_string(tstr_1 STRING, tstr_2 STRING, tstr_3 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '../data/files/test_udf_greatest_string' OVERWRITE INTO TABLE test_greatest_string;
select greatest(tstr_1,tstr_2,tstr_3),tstr_1,tstr_2,tstr_3 from test_greatest_string order by tstr_1;
DROP TABLE test_greatest_string; 

CREATE TABLE test_greatest_int(tstr_1 int, tstr_2 int, tstr_3 int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '../data/files/test_udf_greatest_int' OVERWRITE INTO TABLE test_greatest_int;
select greatest(tstr_1,tstr_2,tstr_3),tstr_1,tstr_2,tstr_3 from test_greatest_int order by tstr_1;
DROP TABLE test_greatest_int; 

CREATE TABLE test_greatest_bigint(tstr_1 bigint, tstr_2 bigint, tstr_3 bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '../data/files/test_udf_greatest_int' OVERWRITE INTO TABLE test_greatest_bigint;
select greatest(tstr_1,tstr_2,tstr_3),tstr_1,tstr_2,tstr_3 from test_greatest_bigint order by tstr_1;
DROP TABLE test_greatest_bigint; 

CREATE TABLE test_greatest_mix(tstr_1 bigint, tstr_2 int, tstr_3 smallint, tstr_4 tinyint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '../data/files/test_udf_greatest_mix' OVERWRITE INTO TABLE test_greatest_mix;
select greatest(tstr_1,tstr_2,tstr_3, tstr_4),tstr_1,tstr_2,tstr_3, tstr_4 from test_greatest_mix order by tstr_1;
DROP TABLE test_greatest_mix;

 
