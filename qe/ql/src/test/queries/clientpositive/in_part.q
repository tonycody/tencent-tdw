DROP TABLE nulltest;
DROP TABLE nulltestdata;

CREATE TABLE nulltest(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) PARTITION BY LIST(int_data1) (partition par_name1 values in (0), partition par_name2 values in (1),partition par_name3 values in (2),PARTITION DEFAULT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


CREATE external TABLE nulltestdata(int_data1 INT, int_data2 INT, boolean_data BOOLEAN, double_data DOUBLE, string_data STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/nulltest.txt' INTO TABLE nulltestdata;

INSERT TABLE nulltest SELECT * FROM nulltestdata;

SELECT * FROM nulltest WHERE int_data1 IN(1);

DROP TABLE nulltest;
DROP TABLE nulltestdata;
