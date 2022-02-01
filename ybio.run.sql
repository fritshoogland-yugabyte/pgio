create or replace procedure ybio.run( p_config_id int, p_schema int default 1, p_run_tag text default 'run' )
language plpgsql as $$
declare
  v_rows bigint;
  v_rows_per_message int;
  v_number_schemas int;
  v_table_f1_range bigint;
  v_table_f2_width int;
  v_run_rows_per_commit bigint;
  v_run_update_pct int;
  v_run_delete_pct int; 
  v_run_range int;
  v_run_time interval;
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
  v_clock_batch timestamp; 
begin

  /*
   * read the configuration from the ybio.config table.
   * raise exception and stop if not found.
   */
  select rows,
         rows_per_message,
         number_schemas,
         table_f1_range,
         table_f2_width,
         run_rows_per_commit,
         run_update_pct,
         run_delete_pct,
         run_range,
         run_time
  into   v_rows,
         v_rows_per_message,
         v_number_schemas,
         v_table_f1_range,
         v_table_f2_width,
         v_run_rows_per_commit,
         v_run_update_pct,
         v_run_delete_pct,
         v_run_range,
         v_run_time
  from   ybio.config
  where  id = p_config_id;
  if not found then
    raise exception 'config id % not found in ybio.config table', p_config_id;
  end if;

  /*
   * if p_schema must be lower or equal than v_number_schemas
   */
  if p_schema > v_number_schemas then
    raise exception 'schema nr % too high, maximal number is %.', p_schema, v_number_schemas;
  end if;

  /*
   * rows_per_message by default is 0, which means we take the figure from run_rows_per_commit.
   * if a value is specified, we round it up to the next run_rows_per_commit.
   */
  if v_rows_per_message = 0 then
    v_rows_per_message := v_run_rows_per_commit;
  else
    v_rows_per_message := (((v_rows_per_message/v_run_rows_per_commit)+1)*v_run_rows_per_commit);
  end if;

  /*
   * the action performed is dependent on a random number taken from 1 to 100.
   * the percentages are set as ranges between 1 and 100, and then v_random decides what to do.
   * v_select_pct is not set, but derived from the remainder of v_update_pct + v_delete_pct.
   */
  v_select_pct_until := 100 - v_run_update_pct - v_run_delete_pct;
  v_update_pct_until := v_select_pct_until + v_run_update_pct;
  v_delete_pct_until := v_update_pct_until + v_run_delete_pct;

  raise notice 'run on schema ybio%, duration: %.', p_schema, v_run_time;
  raise notice 'work ratios select / update / delete: % / % / %', v_select_pct_until, v_run_update_pct, v_run_delete_pct;
  raise notice 'selected id range in scan: %', v_run_range;
  execute format('set search_path to ybio%s', p_schema);
  v_clock_batch := clock_timestamp();

  /*
   * main loop
   */
  while clock_timestamp() < v_clock_begin + v_run_time loop

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
          select id, f1, f2 into v_dummy_id, v_dummy_f1, v_dummy_f2 from benchmark_table where id between v_random_row and v_random_row+v_run_range-1;
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
          --update benchmark_table set f1=dbms_random.value(1,v_table_f1_range), f2=dbms_random.string('a',v_table_f2_width) where id = v_random_row;
          update benchmark_table set f1=dbms_random.value(1,v_table_f1_range), 
                                     f2=dbms_random.string('a',v_table_f2_width), 
                                     f3=dbms_random.string('a',v_table_f2_width), 
                                     f4=dbms_random.string('a',v_table_f2_width), 
                                     f5=dbms_random.string('a',v_table_f2_width), 
                                     f6=dbms_random.string('a',v_table_f2_width), 
                                     f7=dbms_random.string('a',v_table_f2_width), 
                                     f8=dbms_random.string('a',v_table_f2_width), 
                                     f9=dbms_random.string('a',v_table_f2_width), 
                                     f10=dbms_random.string('a',v_table_f2_width) 
                                 where id = v_random_row;
        else
          --update benchmark_table set f1=dbms_random.value(1,v_table_f1_range), f2=dbms_random.string('a',v_table_f2_width) where id between v_random_row and v_random_row+v_run_range-1;
          update benchmark_table set f1=dbms_random.value(1,v_table_f1_range), 
                                     f2=dbms_random.string('a',v_table_f2_width), 
                                     f3=dbms_random.string('a',v_table_f2_width), 
                                     f4=dbms_random.string('a',v_table_f2_width), 
                                     f5=dbms_random.string('a',v_table_f2_width), 
                                     f6=dbms_random.string('a',v_table_f2_width), 
                                     f7=dbms_random.string('a',v_table_f2_width), 
                                     f8=dbms_random.string('a',v_table_f2_width), 
                                     f9=dbms_random.string('a',v_table_f2_width), 
                                     f10=dbms_random.string('a',v_table_f2_width) 
                                 where id between v_random_row and v_random_row+v_run_range-1;
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
          delete from benchmark_table where id between v_random_row and v_random_row+v_run_range-1;
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
    if mod(v_select_counter+v_update_counter+v_delete_counter+v_notfound_counter, v_run_rows_per_commit) = 0 then
      commit;
    end if;

    /*
     * report progress.
     */
    if mod(v_select_counter+v_update_counter+v_delete_counter+v_notfound_counter, v_rows_per_message) = 0 then
      raise notice '% sec, rows: sel %, upd %, del %, nfd %, avg: % p/s, avg lat % s',
        to_char(round(extract(epoch from clock_timestamp()-v_clock_begin)),'999G999G999'),
        to_char(v_select_counter,'999G999G999'),
        to_char(v_update_counter,'999G999G999'),
        to_char(v_delete_counter,'999G999G999'),
        to_char(v_notfound_counter,'999G999G999'),
        to_char(v_rows_per_message/extract(epoch from clock_timestamp()-v_clock_batch),'999G999'),
        to_char(extract(epoch from clock_timestamp()-v_clock_batch)/v_rows_per_message,'999.999G999');
      v_clock_batch := clock_timestamp();
    end if;

  end loop;

  /*
   * end of run summary.
   */
  v_clock_end := clock_timestamp();
  raise notice 'finished run on schema ybio%, duration: %.', p_schema, v_run_time;
  raise notice 'rows per commit: %, rows per message: %', v_run_rows_per_commit, v_rows_per_message;
  raise notice '% sec, rows: sel %, upd %, del %, nfd %, avg: % p/s, avg lat % s',
    to_char(round(extract(epoch from clock_timestamp()-v_clock_begin)),'999G999G999'),
    to_char(v_select_counter,'999G999G999'),
    to_char(v_update_counter,'999G999G999'),
    to_char(v_delete_counter,'999G999G999'),
    to_char(v_notfound_counter,'999G999G999'),
    to_char((v_select_counter+v_update_counter+v_delete_counter+v_notfound_counter)/extract(epoch from clock_timestamp()-v_clock_begin),'999G999'),
    to_char(extract(epoch from clock_timestamp()-v_clock_begin)/(v_select_counter+v_update_counter+v_delete_counter+v_notfound_counter),'999.999G999');
  raise notice '                     %: sel %, upd %, del %',
    '%',
    to_char(v_select_pct_until,'999G999G999'),
    to_char(v_run_update_pct,'999G999G999'),
    to_char(v_run_delete_pct,'999G999G999');

  insert into ybio.results 
    (config_id, start_time, end_ttime, inet_server_addrs, pg_backend_pid, nr_total, nr_insert, nr_select, nr_update, nr_delete, nr_notfound, run_tag )
    values
    (p_config_id, v_clock_begin, v_clock_end, inet_server_addrs(), pg_backed_pid(), (v_select_counter+v_update_counter+v_delete_counter+v_notfound_counter), 0, v_select_counter, v_update_counter, v_delete_counter, v_notfound_counter,  p_run_tag);
   

end $$;
