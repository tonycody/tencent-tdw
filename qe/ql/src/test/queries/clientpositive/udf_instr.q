EXPLAIN 
 SELECT instr('abcd', 'abc'),
       instr('abcabc', 'ccc'),
       instr(123, '23'),
       instr(123, 23),
       instr('12345', CAST('2' AS TINYINT)),
       instr(CAST('12345' AS SMALLINT), '34'),
       instr(CAST('123456789012' AS BIGINT), '456'),
       instr(CAST(1.25 AS FLOAT), '.25'),
       instr(CAST(16.0 AS DOUBLE), '.0'),
       instr(null, 'abc'),
       instr('abcd', null)
FROM src LIMIT 1;

SELECT instr('abcd', 'abc'),
       instr('abcabc', 'ccc'),
       instr(123, '23'),
       instr(123, 23),       
       instr('12345', CAST('2' AS TINYINT)),
       instr(CAST('12345' AS SMALLINT), '34'),
       instr(CAST('123456789012' AS BIGINT), '456'),
       instr(CAST(1.25 AS FLOAT), '.25'),
       instr(CAST(16.0 AS DOUBLE), '.0'),
       instr(null, 'abc'),
       instr('abcd', null)
FROM src LIMIT 1;

SELECT instr('abcd', 'abc', 1),
			 instr('abcd', 'abc', 2),
			 instr('abcd', 'abc', 0),
			 instr('abcd', 'abc', 1, 1),
			 instr('abcd', 'abc', 1, 2),       
       instr(null, 'abc',1),
       instr(null, 'abc',1,1)       
FROM src LIMIT 1;
