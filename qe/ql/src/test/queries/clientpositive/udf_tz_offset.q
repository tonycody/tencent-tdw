SELECT
  tz_offset("America/Los_Angeles"),
  tz_offset("US/Michigan"),
  tz_offset(sessiontimezone()),
  tz_offset("GMT-8"),
  tz_offset("dfafasdfasf"),
  tz_offset("GMT-8:00"),
  tz_offset("-08:00"),
  tz_offset("08:00"),
  tz_offset("+08:00"),
  tz_offset("-8:00")
FROM src LIMIT 1; 

CREATE TABLE test_tz_offset(tstr STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/test_udf_tz_offset' OVERWRITE INTO TABLE test_tz_offset; 

SELECT * 
FROM test_tz_offset;

SELECT 
  tz_offset(test_tz_offset.tstr)
FROM test_tz_offset;

DROP TABLE test_tz_offset; 

