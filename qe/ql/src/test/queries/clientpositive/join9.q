CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;


EXPLAIN EXTENDED
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value where src1.ds = '2008-04-08' and src1.hr = '12';

FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value where src1.ds = '2008-04-08' and src1.hr = '12';

SELECT dest1.* FROM dest1;

EXPLAIN EXTENDED
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value where src1.ds = '2008-04-08' and src1.hr = '18';

FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value where src1.ds = '2008-04-08' and src1.hr = '18';

SELECT dest1.* FROM dest1;
