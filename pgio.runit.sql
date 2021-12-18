create or replace procedure pgio.runit( p_config_id int, p_schema int default 1, p_runtime interval default interval '1 minute' )
language plpgsql as $$
declare
  v_rows bigint;
  v_create_number_schemas int;
  v_table_f1_range bigint;
  v_table_f2_width int;
  v_run_rows_per_commit bigint;
  v_run_update_pct int;
  v_run_delete_pct int; 
  v_run_range int;
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
  v_clock_batch timestamp; 
begin

  /*
   * read the configuration from the pgio.config table.
   * raise exception and stop if not found.
   */
  select rows,
         create_number_schemas,
         table_f1_range,
         table_f2_width,
         run_rows_per_commit,
         run_range,
         update_pct,
         delete_pct
  into   v_rows,
         v_create_number_schemas,
         v_table_f1_range,
         v_table_f2_width,
         v_run_rows_per_commit,
         v_run_range,
         v_run_update_pct,
         v_run_delete_pct
  from   pgio.config
  where  id = p_config_id;
  if not found then
    raise exception 'config id % not found in pgio.config table', p_config_id;
  end if;

  /*
   * if p_schema must be lower or equal than v_create_number_schemas
   */
  if p_schema > v_create_number_schemas then
    raise exception 'schema nr % too high, maximal number is %.', p_schema, v_create_number_schemas;
  end if;

  /*
   * the action performed is dependent on a random number taken from 1 to 100.
   * the percentages are set as ranges between 1 and 100, and then v_random decides what to do.
   * v_select_pct is not set, but derived from the remainder of v_update_pct + v_delete_pct.
   */
  v_select_pct_until := 100 - v_update_pct - v_delete_pct;
  v_update_pct_until := v_select_pct_until + v_update_pct;
  v_delete_pct_until := v_update_pct_until + v_delete_pct;

  raise notice 'run on schema pgio%, duration: %.", p_schema, p_runtime;
  raise notice 'work ratios select / update / delete: % / % / %', v_select_pict_until, v_update_pct, v_delete_pct;
  execute format('set search_path to pgio%s', p_schema);
  v_clock_batch := clock_timestamp();

  /*
   * main loop
   */
  while clock_timestamp() < v_clock_begin + v_runtime loop

    /*
     * v_random selects the type of work based on percentages.
     * v_random_row select the id of the row.
     * Because of deletes, v_random_row could be deleted already. Therefore, a statistic v_notfound_counter is kept.
     */
    v_random := dbms_random.value(1,100);
    v_random_row := dbms_random.value(1,v_rows-v_run_range);   

    case 
 
      /*
       * select
       */
      when v_random <= v_select_pct_until then 
        if v_run_range = 1 then
          select id, f1, f2 into v_dummy_id, v_dummy_f1, v_dummy_f2 from benchmark_table where id = v_random_row;
        else
          select id, f1, f2 into v_dummy_id, v_dummy_f1, v_dummy_f2 from benchmark_table where id between v_random_row and v_random_row+v_run_range;
        end if;
        if not found then
          v_notfound_counter := v_notfound_counter + 1;
        else
          v_select_counter := v_select_counter + v_run_range;
        end if;

      /*
       * update
       */
      when v_random <= v_update_pct_until then 
        if v_run_range = 1 then
          update benchmark_table set f1=dbms_random.value(1,v_table_f1_range), f2=dbms_random.string('a',v_table_f2_width) where id = v_random_row;
        else
          update benchmark_table set f1=dbms_random.value(1,v_table_f1_range), f2=dbms_random.string('a',v_table_f2_width) where id between v_random_row and v_random_row+v_run_range;
        end if;
        if not found then
          v_notfound_counter := v_notfound_counter + 1;
        else
          v_update_counter := v_update_counter + v_run_range;
        end if;

      /*
       * delete
       */
      when v_random <= v_delete_pct_until then
        if v_run_range = 1 then
          delete from benchmark_table where id = v_random_row;
        else
          delete from benchmark_table where id between v_random_row and v_random_row+v_run_range;
        end if;
        if not found then
          v_notfound_counter := v_notfound_counter +1;
        else
          v_delete_counter := v_delete_counter + v_run_range;
        end if;

    end case;

    /*
     * commit per v_run_rows_per_commit. it doesn't make sense to commit for selects, but it shouldn't matter.
     */
    if mod(v_select_counter+v_update_counter+v_delete_counter, v_run_rows_per_commit) = 0 then
      commit;
    end if;

    /*
     * report progress.
     */
    if mod(v_select_counter+v_update_counter+v_delete_counter, v_run_rows_per_message) = 0 then
      raise notice 'runtime: % seconds, rows: select/update/delete/notfound: %/%/%/%, average: % per second', round(extract(epoch from clock_timestamp()-v_clock_begin)), v_select_counter, v_update_counter, v_delete_counter, v_notfound_counter, to_char(v_run_rows_per_message)/extract(epoch from clock_timestamp()-v_clock_batch)),'99999999');
      v_clock_batch := clock_timestamp();
    end if;

  end loop;

  raise notice 'run on schema pgio%, duration: % finished.", p_schema, p_runtime;
  raise notice 'work ratios select / update / delete: % / % / %', v_select_pict_until, v_update_pct, v_delete_pct;
  raise notice 'rows per commit: %, rows per message: %', v_run_rows_per_commit, v_run_rows_per_message;
  raise notice 'rows processed: select / update / delete / notfound: % / % / % / %', v_select_counter, v_update_counter, v_delete_counter, v_notfound_counter;
  raise notice 'total time: %, average number of rows per second: %', 
    round(extract(epoch from clock_timestamp()-v_clock_begin)::numeric,2), 
    to_char(round((v_select_counter+v_update_counter+v_delete_counter)/extract(epoch from clock_timestamp()-v_clock_begin)),'99999999');
