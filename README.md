This is the yugabyte version of pgio.
This is a toolkit meant for performing Yugabyte YSQL database transactions.

To install:
\i setup.sql

To remove:
\i uninstall.sql
(uninstall does not remove the orafce extension)

After installation you have to insert at least one row in the table pgio.config.
The table pgio.config has default values for every column, so you only have to change the fields you want different:

rows 				        bigint  default 1000000,
create_batch_size 	        bigint  default 1000,
number_schemas		        int     default 1,
table_primary_key		    boolean default true,
table_primary_key_type	    text    default 'hash',
table_tablets			    int     default 0,
table_f1_range		        bigint  default 1000000,
index_f1			        boolean default false,
index_f1_type			    text    default 'hash',
index_f1_tablets		    int     default 0,
table_f2_width		        int     default 100,
run_batch_size		        bigint  default 1000,
update_pct			        int     default 0,
delete_pct			        int     default 0

Frits Hoogland, Yugabyte.