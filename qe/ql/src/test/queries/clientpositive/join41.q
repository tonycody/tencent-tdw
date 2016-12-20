EXPLAIN
SELECT /*+ MAPJOIN(a) */ count(1) FROM src a  JOIN src b on a.key = b.key;

SELECT /*+ MAPJOIN(a) */ count(1) FROM src a  JOIN src b on a.key = b.key;
