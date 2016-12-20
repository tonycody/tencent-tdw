set fs.default.name=invalidscheme:///;
set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;


DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table3;

CREATE TABLE table1 (a STRING, b STRING) STORED AS TEXTFILE;
DESCRIBE table1;
DESCRIBE EXTENDED table1;

CREATE TABLE IF NOT EXISTS table1 (a STRING, b STRING) STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS table2 (a STRING, b INT) STORED AS TEXTFILE;
DESCRIBE table2;
DESCRIBE EXTENDED table2;

CREATE TABLE table3 (a STRING, b STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
DESCRIBE table3;
DESCRIBE EXTENDED table3;

DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table3;
