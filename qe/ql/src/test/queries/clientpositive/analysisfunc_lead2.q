DROP TABLE businesssrc;
set analysisbuffer.tmp.addr=/tmp;
CREATE TABLE businesssrc(cust_num INT, region_id INT, saler_id INT, year INT, month INT, tot_orders INT, tot_sales INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../data/files/saledata.txt' INTO TABLE businesssrc;

EXPLAIN 
FROM businesssrc
SELECT region_id, cust_num, saler_id, year, month, tot_sales, LEAD(tot_sales, 3) OVER(partition by region_id order by year, month);

SELECT * FROM (SELECT region_id, cust_num, saler_id, year, month, tot_sales, LEAD(tot_sales, 3) OVER(partition by region_id order by year, month) FROM businesssrc) tmp ORDER BY region_id, cust_num, saler_id, year, month;

DROP TABLE businesssrc;