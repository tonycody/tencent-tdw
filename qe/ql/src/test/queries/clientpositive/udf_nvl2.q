SELECT
  nvl2(null,"test2","test3"),
  nvl2(null,null, "test3"),
  nvl2(null, null, null),
  nvl2("test1", "test2", "test3")
 FROM src LIMIT 1;  

SELECT
  nvl2(null,2,3),
  nvl2(null,null, 3),
  nvl2(null, null, null),
  nvl2(1, 2, 3)
 FROM src LIMIT 1; 
 
 SELECT
  nvl2(null,2.2,3.3),
  nvl2(null,null, 3.4),
  nvl2(null, null, null),
  nvl2(1.1, 2.2, 3.3)
 FROM src LIMIT 1; 