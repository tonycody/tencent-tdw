drop table hashtable1;
create table hashtable1 (col_name1 int, col_name2 int) partition by hashkey(col_name2) subpartition by list(col_name1)  (partition par_name1 values in (1,3,5), partition par_name2 values in (2,4,6), partition default);
