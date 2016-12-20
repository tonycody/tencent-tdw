DROP TABLE testtbl;
CREATE TABLE testtbl(KEY STRING, VALUE STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE testtbl;
explain
select count(1) from testtbl;
select count(1) from testtbl;

DROP TABLE testtbl2;
CREATE TABLE testtbl2(KEY STRING, VALUE STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/nullfile.txt' INTO TABLE testtbl2;
explain
select count(1) from testtbl2;
select count(1) from testtbl2;

DROP TABLE testtbl;
CREATE TABLE testtbl(KEY STRING, VALUE STRING)STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE testtbl ;
LOAD DATA LOCAL INPATH '../data/files/nullfile.txt' INTO TABLE testtbl;
explain
select count(1) from testtbl;
select count(1) from testtbl;

DROP TABLE testtbl;
DROP TABLE testtbl2;
