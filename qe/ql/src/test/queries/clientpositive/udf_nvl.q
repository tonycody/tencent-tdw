SELECT
  nvl(null,"konten"),
  nvl("konten", "aa"),
  nvl (null, null)
 FROM src LIMIT 1;  

SELECT
  nvl(null,3),
  nvl(2, 1),
  nvl (null, null)
 FROM src LIMIT 1; 
 
 SELECT
  nvl(null,3.4),
  nvl(2.5, 1.1),
  nvl (null, null)
 FROM src LIMIT 1; 