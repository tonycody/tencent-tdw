set pg_url = 127.0.0.1:5432/tdw_query_info;  

create external table pgdata1 (key int,value string) stored as pgdata;

desc extended pg_data1;

explain select * from pgdata1 order by key;

select * from pgdata1 order by key;

set hive.pgdata.storage.url = jdbc:postgresql://127.0.0.1:5432/tdw_query_info;

create external table pgdata2 (key int,value string) stored as pgdata;

desc extended pgdata2;

explain select * from pgdata2 order by key;

select * from pgdata2 order by key;

explain select * from pgdata2 where key < 10;

select * from pgdata2 where key < 10;
