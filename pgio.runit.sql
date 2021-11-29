create or replace procedure pgio.runit( v_schema int, v_config_id int, v_runtime interval default interval '1 minute' )
language plpgsql as $$
declare
  v_rows bigint;
  v_table_f1_range bigint;
  v_table_f2_width int;
  v_run_batch_size bigint;
  v_update_pct int;
  v_delete_pct int; 
  v_select_pct_until int;
  v_update_pct_until int;
  v_delete_pct_until int;
  v_random int;
  v_random_row bigint;
  v_dummy_id bigint;
  v_dummy_f1 bigint;
  v_dummy_f2 text;
  v_select_counter bigint := 0;
  v_update_counter bigint := 0;
  v_delete_counter bigint := 0;
  v_notfound_counter bigint := 0;
  v_clock_begin timestamp := clock_timestamp();
  v_clock_end timestamp;
begin
  execute format('set search_path to pgio%s', v_schema);
  select rows,
         table_f1_range,
         table_f2_width,
         run_batch_size,
         update_pct,
         delete_pct
  into   v_rows,
         v_table_f1_range,
         v_table_f2_width,
         v_run_batch_size,
         v_update_pct,
         v_delete_pct
  from   pgio.config
  where  id = v_config_id;
  if not found then
    raise exception 'config id % not found in pgio.config table', v_config_id;
  end if;
  v_select_pct_until := 100 - v_update_pct - v_delete_pct;
  v_update_pct_until := v_select_pct_until + v_update_pct;
  v_delete_pct_until := v_update_pct_until + v_delete_pct;
  raise notice 'starting a run with select/update/delete ratios: %/%/%, batch_size %, schema pgio%, duration: %', v_select_pct_until, v_update_pct, v_delete_pct, v_run_batch_size, v_schema, v_runtime;
  while clock_timestamp() < v_clock_begin + v_runtime loop
    v_random := dbms_random.value(1,100);
    v_random_row := dbms_random.value(1,v_rows-v_run_batch_size);   
    case 
      when v_random <= v_select_pct_until then 
        select id, f1, f2 into v_dummy_id, v_dummy_f1, v_dummy_f2 from benchmark_table where id between v_random_row and v_random_row+v_run_batch_size;
        if not found then
          v_notfound_counter := v_notfound_counter +1;
        else
          v_select_counter := v_select_counter + v_run_batch_size;
        end if;
      when v_random <= v_update_pct_until then 
        update benchmark_table set f1=dbms_random.value(1,v_table_f1_range), f2=dbms_random.string('a',v_table_f2_width) where id between v_random_row and v_random_row+v_run_batch_size;
        if not found then
          v_notfound_counter := v_notfound_counter +1;
        else
          v_update_counter := v_update_counter + v_run_batch_size;
        end if;
      when v_random <= v_delete_pct_until then
        delete from benchmark_table where id between v_random_row and v_random_row+v_batch_size;
        if not found then
          v_notfound_counter := v_notfound_counter +1;
        else
          v_delete_counter := v_delete_counter + v_run_batch_size;
        end if;
    end case;
  end loop;
  v_clock_end := clock_timestamp();
  raise notice 'total time: %, batch size: %, select/update/delete/notfound: %/%/%/%, average: % per second', round(extract(epoch from v_clock_end-v_clock_begin)), v_run_batch_size, v_select_counter, v_update_counter, v_delete_counter, v_notfound_counter, to_char(round((v_select_counter+v_update_counter+v_delete_counter)/extract(epoch from v_clock_end-v_clock_begin)),'99999999');
end $$;