SELECT
  next_day(to_date("2011-01-05", "yyyy-mm-dd"),2),
  next_day(to_date("2011-01-05", "yyyy-mm-dd"),7),
  next_day(to_date("2011-01-05", "yyyy-mm-dd"),0),
  next_day(to_date("2011-01-05", "yyyy-mm-dd"),8)
 FROM src LIMIT 1;  

