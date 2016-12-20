set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

CREATE TABLE part_special (
  a STRING,
  b STRING,
  ds STRING,
  ts STRING
) partition by list(ds) subpartition by list(ts)
(subpartition sp0 values in ('10:11:12=455'))
(partition p0 values in ('2008 04 08'));

EXPLAIN
INSERT OVERWRITE TABLE part_special
SELECT 1, 2, '2008 04 08', '10:11:12=455' FROM src LIMIT 1;

INSERT OVERWRITE TABLE part_special
SELECT 1, 2, '2008 04 08', '10:11:12=455' FROM src LIMIT 1;

DESCRIBE EXTENDED part_special;

SELECT * FROM part_special WHERE ds='2008 04 08' AND ts = '10:11:12=455';

DROP TABLE part_special;
