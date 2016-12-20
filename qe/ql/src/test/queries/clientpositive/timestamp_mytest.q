drop table test_1;
CREATE TABLE test_1(
    i INT,
    b BIGINT,
    d DOUBLE,
    s STRING,
    t TIMESTAMP
);
insert into  test_1 values (1 , 2 ,3.0 ,4 ,"1989-06-04 12:12:12");
select cast(cast(unix_timestamp(t)*1000 as bigint)as timestamp),t from test_1;
select to_utc_timestamp(t,"GMT+2"),t from test_1;
select from_utc_timestamp(t,"GMT+2"),t from test_1;
select cast(233423.23233443 as timestamp) from test_1;
select cast(233423 as timestamp) from test_1;
insert into test_1  values (2,2,2,2,"1999-09-09 11:22:33");
select std(t) from test_1;

