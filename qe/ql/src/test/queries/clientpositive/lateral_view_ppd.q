set hive.optimize.ppd=true;

EXPLAIN SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE key='0';
SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE key='0';

EXPLAIN SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE key='0' AND myCol=1;
SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE key='0' AND myCol=1;


EXPLAIN SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array(1,2,3)) myTable2 AS myCol2) a WHERE key='0';
SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array(1,2,3)) myTable2 AS myCol2) a WHERE key='0';
