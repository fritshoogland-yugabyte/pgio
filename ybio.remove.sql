create or replace procedure ybio.remove ( p_config_id int )
language plpgsql as $$
declare
  v_number_schemas int;
begin
  select number_schemas 
  into   v_number_schemas 
  from   ybio.config 
  where  id = p_config_id;
  if not found then 
    raise exception 'config id % not found in ybio.config table', config_id;
  end if;
  for v_schema_nr in 1..v_number_schemas loop
    execute format('drop schema if exists ybio%s cascade', v_schema_nr);
  end loop;
end $$;
