create or replace procedure pgio.remove ( config_id int )
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
begin
  select number_schemas, 
         rows, 
         create_batch_size, 
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
    -- drop schema 
    execute format('drop schema if exists pgio%s cascade', schema_nr);
  end loop;
end $$;