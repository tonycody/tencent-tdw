SELECT
  bitand(3, 7),
  bitand(3, 10),
  bitand(0, 10),
  bitand(10,0),
  bitand(null,0),
  bitand(0,null),
  bitand(null,null)
 FROM src LIMIT 1;  

