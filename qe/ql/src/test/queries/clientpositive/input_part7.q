EXPLAIN EXTENDED  SELECT * FROM (  SELECT X.* FROM SRCPART X WHERE X.ds = '2008-04-08' and X.key < 100  UNION ALL  SELECT Y.* FROM SRCPART Y WHERE Y.ds = '2008-04-08' and Y.key < 100) A SORT BY A.key;

SELECT * FROM (  SELECT X.* FROM SRCPART X WHERE X.ds = '2008-04-08' and X.key < 100  UNION ALL  SELECT Y.* FROM SRCPART Y WHERE Y.ds = '2008-04-08' and Y.key < 100) A SORT BY A.key, A.hr;