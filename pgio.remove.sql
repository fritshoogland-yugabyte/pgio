create or replace procedure pgio.remove ( p_config_id int )
language plpgsql as $$
declare
  v_create_number_schemas int;
begin
  select create_number_schemas, 
  into   v_create_number_schemas, 
  from   pgio.config 
  where  id = p_config_id;
  if not found then 
    raise exception 'config id % not found in pgio.config table', config_id;
  end if;
  for v_schema_nr in 1..v_create_number_schemas loop
    execute format('drop schema if exists pgio%s cascade', v_create_schema_nr);
  end loop;
end $$;
