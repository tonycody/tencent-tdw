CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;
set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\002'
  USING '/bin/cat'
  AS (tkey, tvalue) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\003'
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tkey, tvalue;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\002'
  USING '/bin/cat'
  AS (tkey, tvalue) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\003'
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tkey, tvalue;

SELECT dest1.* FROM dest1;
