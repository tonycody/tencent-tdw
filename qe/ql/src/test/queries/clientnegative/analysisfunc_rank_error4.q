DROP TABLE businesssrc;

CREATE TABLE businesssrc(cust_num INT, region_id INT, saler_id INT, year INT, month INT, tot_orders INT, tot_sales INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

FROM businesssrc
SELECT cust_num, region_id, saler_id, year, month, tot_orders, tot_sales, RANK() OVER(partition by region_id order by year), LEAD(tot_sales) OVER(partition by region_id order by year, month);

DROP TABLE businesssrc;