-- pgio.config
-- this table is empty after install.
-- every column has a default value, so you have to specify only what you want to be different.
-- such as: insert into pgio.config (rows) values (1000000);
-- 
-- this gives all kinds of options for testing, like:
-- * changing the PK and field f1 index type (from 'hash' to 'asc' or 'desc').
-- * enabling an index on field f1 (defafult false (not)).
-- * changing the creation commit batch size (create_batch_size).
-- * changing the run commit batch size (run_batch_size).
-- * the number of tablets for the table (table_tablets)
-- * the number of tablets for the index on f1 (index_f1_tablets)
-- * the number of updates as a percentage during the run (runit)
-- * the number of deletes as a percentage during the run (runit)
-- * the remainder of the actions is a select (runit)
create table pgio.config (
  id 				        serial  primary key,
  rows 				        bigint  default 1000000,
  create_batch_size 	    bigint  default 1000,
  number_schemas		    int     default 1,
  table_primary_key		    boolean default true,
  table_primary_key_type	text    default 'hash',
  table_tablets			    int     default 0,
  table_f1_range		    bigint  default 1000000,
  index_f1			        boolean default false,
  index_f1_type			    text    default 'hash',
  index_f1_tablets		    int     default 0,
  table_f2_width		    int     default 100,
  run_batch_size		    bigint  default 1000,
  update_pct			    int     default 0,
  delete_pct			    int     default 0
);
-- this table is not really used yet.
create table pgio.results (
  run_id		            serial,
  start_time		        timestamp,
  end_time		            timestamp
);
