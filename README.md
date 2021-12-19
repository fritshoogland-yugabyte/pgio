This is the yugabyte version and rewrite of pgio: ybio.
This is a toolkit meant for performing Yugabyte YSQL database transactions.
This toolkit is strongly inspired on Kevin Closson's SLOB and pgio tools/projects, and the work Franck Pachot based on that.

# Dependencies
This toolkit uses the orafce extension. The reason is to generate random data with low CPU usage, I've found the postgres native random function to be CPU demanding.
The orafce extension is delivered as part of a default installation of YugabyteDB.

# Installation/deinstallation

To install:
```
\i install.sql
```

To remove:
```
\i uninstall.sql
```
Please mind:  
- uninstall.sql will not remove any schema's with the benchmark_table table in it.  
- uninstall does not remove the orafce extension, if you want to remove that, run `drop extension orafce;`.  

# Setup
In order to use ybio, you must follow the following steps:
1. Insert or identify a config in the ybio.config table.
2. Setup/create the configuration using the ybio.setup prodcedure.
3. Run the configuration using the ybio.run procedure.

## ybio.config, create a configuration
After installation at least one row must be in the ybio.config table to hold the definition of a configuration, because the setup, run and remove procedures use the table to obtain the configuration details.
This means it's possible to create a second configuration with slightly changed configuration details to perform a run on, the ybio.config.id number has no direct relationship with the ybio<nr>.benchmark_table table. Of course this means it's up to you to make sure the details of a config.id are consistent with the setup schema's.  

The table ybio.config has default values for every column, so you only have to change the fields you want to have a different value for:
```
  id 				    serial  primary key,
  rows 				    bigint  default 1000000,
  rows_per_message                  int     default 0,
  number_schemas		    int     default 1,
  create_rows_per_commit 	    bigint  default 1000,
  create_method                     text    default 'unnest',
  table_primary_key		    boolean default true,
  table_primary_key_type	    text    default 'hash',
  table_tablets			    int     default 0,
  table_f1_range		    bigint  default 1000000,
  table_f2_width		    int     default 100,
  index_f1			    boolean default false,
  index_f1_type			    text    default 'hash',
  index_f1_tablets		    int     default 0,
  run_rows_per_commit		    bigint  default 10000,
  run_update_pct		    int     default 0,
  run_delete_pct		    int     default 0,
  run_range                         int     default 1
```
The ybio.config table is ordered by 5 category type of fields:
1. The first 4 fields are general fields, and apply to setup and run.
2. The 'create' fields apply to setup only.
3. The 'table' fields contain properties of the ybio<nr>.benchmark_table that is created.
4. The 'index' fields contain properties of the ybio<nr>.benchmark_table secondary index, if created.
5. The 'run' fields apply to the run procedure only. 

- id: this is an automatically generated number, if you insert a row a number is created. Do not specify an id yourself during inserting a row.
- rows: this fields sets the number of rows that are inserted into the ybio<nr>.benchmark_table tables, and is used for the run procedure to understand how many rows the ybio<nr>.benchmark_table table contains.
- rows_per_message: this field sets the number of rows after which a message is printed about progress for both setup and run. The value of 0 means it takes the create_rows_per_commit or run_rows_per_commit value. If the number is lower or not an exact multiple of create_rows_per_commit or run_rows_per_commit, it is rounded to the next value of that.
- number_schemas: this option sets the number of schemas that are or should be created. 
- create_rows_per_commit: this fields sets the number of rows after which a commit is executed. For the unnest create_method, this is the array size.
- create_method: possible values: unnest (default) or row. Unnest produces rows from an array using insert into <table> select unnest(array). Row simply produces single insert commands.
- table_primary_key: this option allows you to specify if you want to create the ybio<nr>.benchmark_table to be created with a primary key defined (default) or not.
- table_primary_key_type: possible values: hash (default), asc, desc. this option allows you to specify the type of the primary key index. This is important, because the primary key defines the ordering of the table.
- table_tablets: this option allows you to specify the number of tablets that are created when the table is created. The default value of 0 does not specify the number of tablets, and thus will go with the database default number.
- table_f1_range: this option sets the number range for the ybio<nr>.benchmark_table.f1 field. This allows you to manipulate the cardinality of looking up a value if an index is created on this field.
- table_f2_width: this option sets the number of characters that are inserted into ybio<nr>.benchmark_table.f2 field. This allows you to size the rows.
- index_f1: this option allows you to have a secondary index created on the ybio<nr>.benchmark_table.f1 field. default false.
- index_f1_type: possible values: hash (default), asc, desc. this option allows you to specify the type of the secondary index. 
- index_f1_tablets: this option allows you to specify the number of tablets that are created when the index is created. The default value of 0 does not specify the number of tablets, and thus will go with the database default number.
- run_rows_per_commit: this option allows you to specify the number of rows before a commit is executed. this number is calculated as loop count * run_range.
- run_update_pct: this option sets the percentage of update statements during ybio.run. Selects are performed for the remainder of 100-run_update_pct-run_delete_pct. You are responsible for making sure run_update_pct+run_delete_pct does not exceed 100.
- run_delete_pct: this option sets the percentage of delete statements during ybio.run. Selects are performed for the remainder of 100-run_update_pct-run_delete_pct. You are responsible for making sure run_update_pct+run_delete_pct does not exceed 100.
- run_range: the number range for an executed select, update or delete during ybio.run. The default value is 1, which means the statements are executed with 'id = <nr>'. Setting it > 1 makes the statements be executed with 'id between <nr> and <nr>'. 

Because every field has a default value, you only have to specify a field that you want to be different from the default value. You have to specify at least one field during an insert statement though.

For example:
1. Create config:
```
insert into ybio.config (rows) values (1000000);
```
2. Verify config:
```
\x
...........dkdkdkd
```
3. Execute setup: 
- Please mind that the default behavior of setup is to drop the schema before creation.
- The first argument for ybio.setup is the ybio.config.id number, and is mandatory.
- The second argument for ybio.setup is optional. If set to a value > 0, it will only setup the schema with the specified number. By default all the schemas are created sequentially. This allows multiple schemas to be created and loaded concurrently.
- The second argument for ybio.setup is optional. If set to a value > 0, it will not drop the schema before executing, and instead will only try to insert rows with numbers multiplied by the set number. This allows multiple processes to insert into the same table concurrently.
```
call ybio.setup(1);
```

# Run
- The first argument is the ybio.config.id number, and is mandatory.
- The second number is optional, and defaults to 1. It allows to specify a number for the schema to run against.
```
call ybio.run(1);
```

# Remove
This is optional. If you want to remove all the schema's for a given configuration manually, you can execute ybio.remove(<ybio.config.id>).
```
call ybio.remove(1);
```

Frits Hoogland, Yugabyte (fhoogland@yugabyte.com).
