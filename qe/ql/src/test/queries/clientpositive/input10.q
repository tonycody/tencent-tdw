CREATE TABLE TEST10(key INT, value STRING, ds STRING, hr INT) 
partition by list(ds) subpartition by range(hr) 
(subpartition sp1 values less than (12),
subpartition sp2 values less than (24))
(partition p1 values in ('2008-04-08'),
partition p2 values in ('2008-04-09'))
STORED AS TEXTFILE;

EXPLAIN
DESCRIBE TEST10;

DESCRIBE TEST10;

DROP TABLE TEST10;

