DROP TABLE dest1;

CREATE TABLE dest1(key INT, value STRING, ds STRING, hr INT)
PARTITION BY list(ds) SUBPARTITION BY range(hr)
(SUBPARTITION sp0 VALUES LESS THAN (12),
SUBPARTITION sp1 VALUES LESS THAN (24))
(PARTITION p0 VALUES IN ('2008-08-01'),
PARTITION p1 VALUES IN ('2008-09-01'))
STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT TABLE dest1 SELECT src.key, src.value, '2008-08-01', 11 WHERE src.key < 100;

EXPLAIN
FROM src
INSERT TABLE dest1 SELECT src.key, src.value, '2008-09-01', 23 WHERE src.key < 100;

FROM src
INSERT TABLE dest1 SELECT src.key, src.value, '2008-08-01', 11 WHERE src.key < 100;
FROM src
INSERT TABLE dest1 SELECT src.key, src.value, '2008-09-01', 23 WHERE src.key < 100;

SELECT count(1) FROM dest1;

FROM src
INSERT TABLE dest1 SELECT src.key, src.value, '2008-08-01', 11 WHERE src.key < 100;
FROM src
INSERT TABLE dest1 SELECT src.key, src.value, '2008-09-01', 23 WHERE src.key < 100;

SELECT count(1) FROM dest1;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value, '2008-08-01', 11 WHERE src.key < 100;

SELECT count(1) FROM dest1;

FROM src
INSERT TABLE dest1 SELECT src.key, src.value, '2008-09-01', 23 WHERE src.key < 100;

SELECT count(1) FROM dest1;

DROP TABLE dest1;
