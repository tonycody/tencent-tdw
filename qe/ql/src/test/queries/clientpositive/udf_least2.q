create table regexp_instr_test(domain_id string);
LOAD DATA LOCAL INPATH '../data/files/domain_id.txt' OVERWRITE INTO TABLE regexp_instr_test;

select least('abc', 'abcd', 'abcde', 'abcdf', null)
,least('abc', 'abc') 
,least('abc', 'abcd')
,least('abc', 'abcd', 'abcde', 'abcdf')
,least(1, 1)
,least(1, 1000)
,least(1, 1000, null)
,least(cast(1 as smallint), 1000)
,least(6, 6.1)
,least(6, 5.9) 
,least(cast (5 as tinyint), cast(6 as smallint), cast(7 as int), cast(8 as bigint))
,least(cast (5 as tinyint), cast(6 as smallint), cast(7 as int), cast(8 as bigint), 7.1) 
,least(1, 2, greatest(3, 4, 5.0), 4)
,least(1, 2, greatest(3, 4, 5), 4)
,least(6, 5, least(3, 4, 5), 4) from regexp_instr_test limit 1; 

drop table regexp_instr_test;

CREATE TABLE test_least_mix(tstr_1 bigint, tstr_2 int, tstr_3 smallint, tstr_4 tinyint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '../data/files/test_udf_greatest_mix' OVERWRITE INTO TABLE test_least_mix;
select least(tstr_1,tstr_2,tstr_3, tstr_4),tstr_1,tstr_2,tstr_3, tstr_4 from test_least_mix order by tstr_1;
DROP TABLE test_least_mix;

