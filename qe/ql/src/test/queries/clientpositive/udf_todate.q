SELECT
  to_date("20100203", "yyyymmdd"),
  to_date("201002", "yyyymm"),
  to_date("2010", "yyyy"),
  to_date("02", "mm"),
  to_date("03", "dd"),
  to_date("2010-02-03", "yyyy-mm-dd"),
  to_date("2010-02", "yyyy-mm"),
  to_date("20100203142332", "yyyymmddhh24miss"),
  to_date("2010-02-03 14:23:32", "yyyy-mm-dd hh24:mi:ss"),
  to_date("142332", "hh24miss"),
  to_date("20100203142332998", "yyyymmddhh24missff3"),
  to_date("20100203 14:23:32:998", "abc"),
  to_date("2010-02-03 14:23:32:998", "yyyy-mm-dd hh24")
 FROM src LIMIT 1;  

