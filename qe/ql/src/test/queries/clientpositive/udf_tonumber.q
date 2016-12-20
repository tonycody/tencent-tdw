SELECT
  to_number("2010"),
  to_number("2010.12"),
  to_number("20abc"),
  to_number("abc")
 FROM src LIMIT 1;  

