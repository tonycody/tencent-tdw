set hive.optimize.ppd=true;
SET hive.outerjoin.supports.filters = true;

EXPLAIN EXTENDED FROM   src a FULL OUTER JOIN   srcpart b  ON (a.key = b.key AND b.ds = '2008-04-08') SELECT a.key, a.value, b.key, b.value WHERE a.key > 10 AND a.key < 50 AND b.key > 15 AND b.key < 85;

FROM   src a FULL OUTER JOIN   srcpart b  ON (a.key = b.key AND b.ds = '2008-04-09') SELECT a.key, a.value, b.key, b.value WHERE a.key > 10 AND a.key < 50 AND b.key > 15 AND b.key < 85;

EXPLAIN EXTENDED FROM   src a FULL OUTER JOIN   srcpart b  ON (a.key = b.key) SELECT a.key, a.value, b.key, b.value WHERE a.key > 10 AND a.key < 50 AND b.key > 15 AND b.key < 85 AND b.ds = '2008-04-08';

FROM   src a FULL OUTER JOIN   srcpart b  ON (a.key = b.key) SELECT a.key, a.value, b.key, b.value WHERE a.key > 10 AND a.key < 50 AND b.key > 15 AND b.key < 85 AND b.ds = '2008-04-08';
