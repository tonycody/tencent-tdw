set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

create table regexp_instr_test(domain_id string);
LOAD DATA LOCAL INPATH '../data/files/domain_id.txt' OVERWRITE INTO TABLE regexp_instr_test;
SELECT REGEXP_INSTR('500 Oracle Parkway, Redwood Shores, CA', '[s|r|p]', 3, 1, 1, 'x')
,REGEXP_INSTR('500 Oracle Parkway, Redwood Shores, CA', '[s|r|p]', 1, 1, 0, 'x')
,REGEXP_INSTR('500 Oracle Parkway, Redwood Shores, CA', 'rk|ac', 1, 1, 1, 'ix') 
,REGEXP_INSTR('500 Oracle Parkway, Redwood Shores, CA', 'rk|ac', 1, 1, 0, 'ix') 
,REGEXP_INSTR('500 Oracle Parkway, Redwood Shores, CA', 'rk|ac', 1, 2, 0, 'ix') 
,REGEXP_INSTR('500 Oracle Parkway, Redwood Shores, CA', '[s|r|p]', 1, 2, 0, 'i')
,REGEXP_INSTR('500 Oracle Parkway, Redwood Shores, CA', '[s|r|p]', 1, 2, 0, 'c') 
FROM regexp_instr_test limit 1;

SELECT distinct domain_id, regexp_instr(domain_id, '\\.', 1, 1, 1) from regexp_instr_test order by domain_id;
SELECT distinct domain_id, regexp_instr(domain_id, '\\.', 1, 3, 0) from regexp_instr_test order by domain_id;
DROP TABLE regexp_instr_test;



