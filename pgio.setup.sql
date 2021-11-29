create or replace procedure pgio.setup ( config_id int )
language plpgsql as $$
declare
  v_number_schemas int;
  v_rows bigint;
  v_create_batch_size bigint;
  v_table_primary_key boolean;
  v_table_primary_key_type text;
  v_table_tablets int;
  v_table_f2_width int;
  v_table_f1_range bigint;
  v_index_f1 boolean;
  v_index_f1_type text;
  v_index_f1_tablets int;
  v_create_method text;
begin
  select number_schemas, 
         rows, 
         create_batch_size, 
         create_method,
         table_primary_key, 
         table_primary_key_type, 
         table_tablets, 
         table_f2_width,
         table_f1_range,
         index_f1,
         index_f1_type,
         index_f1_tablets
  into   v_number_schemas, 
         v_rows, 
         v_create_batch_size, 
         v_create_method,
         v_table_primary_key, 
         v_table_primary_key_type, 
         v_table_tablets, 
         v_table_f2_width,
         v_table_f1_range,
         v_index_f1,
         v_index_f1_type,
         v_index_f1_tablets
  from   pgio.config 
  where  id = config_id;
  if not found then 
    raise exception 'config id % not found in pgio.config table', config_id;
  end if;
  for schema_nr in 1..v_number_schemas loop
    -- drop schema if it already exists
    execute format('drop schema if exists pgio%s cascade', schema_nr);
    -- create schema
    execute format('create schema pgio%s', schema_nr);
    -- set search_path to schema, so the table is created in the schema
    execute format('set search_path to pgio%s', schema_nr);
    -- create the table
    execute format('create table benchmark_table ( id bigint, f1 bigint, f2 text %s ) %s',
      case v_table_primary_key when true then format(', primary key ( id %s)', v_table_primary_key_type) else '' end, 
      case v_table_tablets when 0 then '' else format('split into %s tablets', v_table_tablets) end
    );
    -- if an index is chosen, create an index
    if v_index_f1 then
      execute format('create index benchmark_table_i_f1 on benchmark_table( f1 %s ) %s',
        v_index_f1_type,
        case v_index_f1_tablets when 0 then '' else format('split into %s tablets', v_index_f1_tablets) end
      );
    end if;
    -- and insert data
    call pgio.insert(v_rows, v_create_batch_size, v_table_f2_width, v_table_f1_range, schema_nr, v_create_method );
  end loop;
end $$;