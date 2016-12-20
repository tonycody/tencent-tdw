drop table myinput1;
CREATE TABLE myinput1(key int, value int);
LOAD DATA LOCAL INPATH '../data/files/in1.txt' INTO TABLE myinput1;

SELECT * FROM myinput1 a JOIN myinput1 b order by a.key, b.key;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b order by a.key, b.key;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b order by a.key, b.key;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.key = b.value;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.key = b.key;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.value = b.value;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key=b.key;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key and a.value=b.value;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key=b.key and a.value = b.value;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.value;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.key;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value and a.key=b.key;

SELECT * from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value) RIGHT OUTER JOIN myinput1 c ON (b.value=c.value);
SELECT * from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value) LEFT OUTER JOIN myinput1 c ON (b.value=c.value);
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.value = c.value;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.key;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.key;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key = b.key;

SELECT * from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value) RIGHT OUTER JOIN myinput1 c ON (b.value=c.value);
SELECT * from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value) LEFT OUTER JOIN myinput1 c ON (b.value=c.value);
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.value = c.value;


DROP TABLE myinput1;
