SELECT commonStr("a,b,c,d","d,c",",") FROM src LIMIT 1;
SELECT commonStr("ac,bd,cf,dg","dg,cf,ak",",") FROM src LIMIT 1;
SELECT commonStr("ac|bd|cf|dg","dg|cf|ak","|") FROM src LIMIT 1;
SELECT commonStr("ac*中国风*摇滚*80后*test","摇滚*ac*ak*中国风","*") FROM src LIMIT 1;