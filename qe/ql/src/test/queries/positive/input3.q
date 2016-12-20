FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200
INSERT OVERWRITE TABLE dest3 SELECT src.key, 2, '2008-04-08', '12' WHERE src.key >= 200 and src.key < 300
INSERT OVERWRITE DIRECTORY '../../../../build/contrib/hive/ql/test/data/warehouse/dest4.out' SELECT src.value WHERE src.key >= 300
