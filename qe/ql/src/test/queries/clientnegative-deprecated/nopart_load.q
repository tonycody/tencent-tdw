DROP TABLE nopart_load;
CREATE TABLE nopart_load(a STRING, b STRING,ds string) PARTITION BY list(ds)(partition p0 values in ('a','b'));

load data local inpath '../data/files/kv1.txt' overwrite into table nopart_load ;

DROP TABLE nopart_load;
