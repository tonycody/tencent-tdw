SELECT
  mod(1, 10),
  mod(0, 10),
  mod(null,10),
  mod(1,0),
  mod(1,null),
  mod(null,null)
 FROM src LIMIT 1;  

