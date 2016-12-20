DROP TABLE test_mapjoin_oom;
CREATE TABLE test_mapjoin_oom(a int);
LOAD DATA LOCAL INPATH '../data/files/mapjoin_oom.txt' INTO TABLE test_mapjoin_oom;
SELECT /*+mapjoin(k)*/ count(*) FROM test_mapjoin_oom k JOIN test_mapjoin_oom b ON (k.a = b.a);
DROP TABLE test_mapjoin_oom;