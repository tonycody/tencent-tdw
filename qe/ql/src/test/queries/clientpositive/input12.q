CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest2(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest3(key INT, ds STRING, hr INT) 
partition by list(ds) subpartition by range(hr) 
(subpartition sp1 values less than (12),
subpartition sp2 values less than (24))
(partition p1 values in ('2008-04-08'),
partition p2 values in ('2008-04-09'))
STORED AS TEXTFILE;

EXPLAIN
FROM src 
INSERT OVERWRITE TABLE dest1 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200;

FROM src 
INSERT OVERWRITE TABLE dest1 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest3 SELECT src.key, '2008-04-08', 12 WHERE src.key >= 200;

FROM src
INSERT OVERWRITE TABLE dest3 SELECT src.key, '2008-04-08', 12 WHERE src.key >= 200;

SELECT dest1.* FROM dest1;
SELECT dest2.* FROM dest2;
SELECT dest3.* FROM dest3;
