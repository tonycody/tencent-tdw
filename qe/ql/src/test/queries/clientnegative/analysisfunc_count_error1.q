DROP TABLE businesssrc;

CREATE TABLE businesssrc(cust_num INT, region_id INT, saler_id INT, year INT, month INT, tot_orders INT, tot_sales INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

FROM businesssrc
SELECT cust_num, region_id, saler_id, year, month, tot_orders, tot_sales, COUNT(tot_sales, tot_orders) OVER(partition by region_id);

DROP TABLE businesssrc;