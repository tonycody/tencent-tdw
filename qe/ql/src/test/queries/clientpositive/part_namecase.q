drop table part_case;
create table part_case(
key int,
value string
) 
partition by list(value)(
partition p1 values in('1','2'),
partition P2 values in('3'),
partition DEfaulT
);

explain
select a.key from
part_case partition(P1) a;

explain
select a.key from
part_case partition(defAult) a;

explain
select a.key from
part_case partition(p2) a;

drop table part_case;
