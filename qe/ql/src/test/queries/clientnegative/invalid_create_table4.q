drop table hashtable1;
create table hashtable1 (col_name1 int, col_name2 int) partition by hashkey(col_name2) subpartition by range(col_name1) (partition par_name1 values less than (2), partition par_name2 values less than (5), partition default);
