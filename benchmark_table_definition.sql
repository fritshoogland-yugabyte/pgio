-- drop schema if exists ybio1 cascade;
-- create schema ybio1;
-- this creates the benchmark table in the ybio1.schema
create table ybio1.benchmark_table ( id bigint, f1 bigint, f2 text, f3 text, f4 text, f5 text, f6 text, f7 text, f8 text, f9 text, f10 text, primary key (id asc)) split at values ((333333),(666666));
