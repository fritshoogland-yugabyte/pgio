This is the yugabyte version of pgio.
This is a toolkit meant for performing Yugabyte YSQL database transactions.
This toolkit is strongly inspired on Kevin Closson's SLOB and pgio tools/projects, and the work Franck Pachot based on that.

To install:
```
\i setup.sql
```

To remove:
```
\i uninstall.sql
```
(uninstall does not remove the orafce extension)

After installation you have to insert at least one row in the table pgio.config.
The table pgio.config has default values for every column, so you only have to change the fields you want different:
```
rows 			        bigint  default 1000000,
create_batch_size 	        bigint  default 1000,
create_method			text	default 'unnest',
number_schemas		        int     default 1,
table_primary_key	    	boolean default true,
table_primary_key_type	    	text    default 'hash',
table_tablets			int     default 0,
table_f1_range		        bigint  default 1000000,
index_f1			boolean default false,
index_f1_type			text    default 'hash',
index_f1_tablets		int     default 0,
table_f2_width		        int     default 100,
run_batch_size		        bigint  default 1000,
update_pct			int     default 0,
delete_pct			int     default 0
```
For example:
1. create config:
```
insert into pgio.config (rows) values (1000000);
```
2. verify config:
```
\x
select * from pgio.config;
-[ RECORD 1 ]----------+--------
id                     | 1
rows                   | 1000000
create_batch_size      | 1000
number_schemas         | 1
table_primary_key      | t
table_primary_key_type | hash
table_tablets          | 0
table_f2_width         | 100
table_f1_range         | 1000000
index_f1               | f
index_f1_type          | hash
index_f1_tablets       | 0
run_batch_size         | 1000
update_pct             | 0
delete_pct             | 0
```
3. setup config: 
```
-- please mind that it will drop the schema prior to creating it.
-- number is pgio.config.id number.
call pgio.setup(1);
```
4. run config:
```
-- first number is config number,
-- second number is schema number.
call pgio.runit(1,1);
```
5. (optionally) remove schema
```
-- number is pgio.config.id number.
call pgio.remove(1);
```

If you specifiy multiple schemas, the setup procedure will fill the schemas sequentially.
If you want to run against multiple schemas at the same time you have to start these manually for each schema.

Frits Hoogland, Yugabyte (fhoogland@yugabyte.com).
