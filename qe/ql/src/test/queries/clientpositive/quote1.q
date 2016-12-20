CREATE TABLE dest1(`location` INT, `type` STRING,`table` string) PARTITION BY list(`table`)(partition p values in ("2008-04-08")) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key as `partition`, src.value as `from` ,'2008-04-08' WHERE src.key >= 200 and src.key < 300;

EXPLAIN
SELECT `int`.`location`, `int`.`type`, `int`.`table` FROM dest1 `int` WHERE `int`.`table` = '2008-04-08';

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key as `partition`, src.value as `from` ,"2008-04-08" WHERE src.key >= 200 and src.key < 300;

SELECT `int`.`location`, `int`.`type`, `int`.`table` FROM dest1 `int` WHERE `int`.`table` = '2008-04-08';
