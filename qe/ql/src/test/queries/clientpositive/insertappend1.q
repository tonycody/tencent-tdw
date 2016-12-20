DROP TABLE dest1;

CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT TABLE dest1 SELECT src.key, src.value WHERE src.key < 100;

FROM src
INSERT TABLE dest1 SELECT src.key, src.value WHERE src.key < 100;

SELECT count(1) FROM dest1;

FROM src
INSERT TABLE dest1 SELECT src.key, src.value WHERE src.key < 100;

SELECT count(1) FROM dest1;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 100;

SELECT count(1) FROM dest1;

DROP TABLE dest1;
