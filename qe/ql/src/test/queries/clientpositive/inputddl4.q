-- a simple test to test sorted/clustered syntax

DROP TABLE INPUTDDL4;
set tolerate.dataerror.write=tolerate;
set tolerate.dataerror.readext=tolerate;
set tolerate.numsplitechar.lessthan.numberfields.exread=tolerate;

CREATE TABLE INPUTDDL4(viewTime STRING, userid INT,
                       page_url STRING, referrer_url STRING, 
                       friends ARRAY<BIGINT>, properties MAP<STRING, STRING>,
                       ip STRING COMMENT 'IP Address of the User',
                       ds STRING, country STRING) 
    COMMENT 'This is the page view table' 
    PARTITION BY list(ds) SUBPARTITION BY list(country)
    (SUBPARTITION sp0 VALUES IN ('CHINA'))
    (PARTITION p0 VALUES IN ('2008-01-01')) 
    CLUSTERED BY(userid) SORTED BY(viewTime) INTO 32 BUCKETS;
DESCRIBE INPUTDDL4;
DESCRIBE EXTENDED INPUTDDL4;
DROP TABLE INPUTDDL4;
