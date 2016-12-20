create table innertab (col1 bigint, col2 string, col3 smallint comment 'smallinteger', col4 string comment 'oldtablecomment') comment 'table:inner';

desc extended innertab;

comment on table innertab is 'new table comment';
desc extended innertab;
comment on table default_db::innertab is null;
desc extended innertab;

comment on column innertab.col1 is 'col1 comment';
desc innertab;
comment on column default_db::innertab.col3 is null;
desc innertab;

drop table innertab;