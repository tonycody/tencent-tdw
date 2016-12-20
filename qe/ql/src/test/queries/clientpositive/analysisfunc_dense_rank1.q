DROP TABLE businesssrc;
set analysisbuffer.tmp.addr=/tmp;
CREATE TABLE businesssrc(cust_num INT, region_id INT, saler_id INT, year INT, month INT, tot_orders INT, tot_sales INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

EXPLAIN
FROM businesssrc
SELECT cust_num, region_id, saler_id, year, month, DENSE_RANK() OVER(partition by region_id order by year asc, month desc);

DROP TABLE businesssrc;