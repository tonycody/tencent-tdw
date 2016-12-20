-- we do not run it because the result may different from each run
explain SELECT
  systimestamp()
 FROM src LIMIT 1;  

