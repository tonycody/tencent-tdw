-- union :map-reduce sub-queries followed by join

explain SELECT unionsrc1.key, unionsrc1.value, unionsrc2.key, unionsrc2.value FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1 UNION  ALL select s2.key , s2.value from src s2 where s2.key < 10) unionsrc1 JOIN  (select 'tst1' as key, cast(count(1) as string) as value from src s3 UNION  ALL select s4.key , s4.value from src s4 where s4.key < 10) unionsrc2 ON (unionsrc1.key = unionsrc2.key);

SELECT unionsrc1.key, unionsrc1.value, unionsrc2.key, unionsrc2.value FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1 UNION  ALL select s2.key , s2.value from src s2 where s2.key < 10) unionsrc1 JOIN  (select 'tst1' as key, cast(count(1) as string) as value from src s3 UNION  ALL select s4.key , s4.value from src s4 where s4.key < 10) unionsrc2 ON (unionsrc1.key = unionsrc2.key);
