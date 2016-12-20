DROP TABLE INPUTDDL8;
CREATE TABLE INPUTDDL8 (ds STRING, country STRING) COMMENT 'This is a thrift based table'
    PARTITION BY list(ds) SUBPARTITION BY list(country)
    (SUBPARTITION sp0 VALUES IN ('CHINA'))
    (PARTITION p0 VALUES IN ('2008-01-01'))
    CLUSTERED BY(aint) SORTED BY(lint) INTO 32 BUCKETS
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
    WITH SERDEPROPERTIES ('serialization.class' = 'org.apache.hadoop.hive.serde2.thrift.test.Complex',
                          'serialization.format' = 'com.facebook.thrift.protocol.TBinaryProtocol')
    STORED AS SEQUENCEFILE;
DESCRIBE EXTENDED INPUTDDL8;
DROP TABLE INPUTDDL8;
