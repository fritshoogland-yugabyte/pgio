create or replace procedure ybio.insert( 
  p_config_id int,
  p_rows bigint, 
  p_create_rows_per_commit bigint, 
  p_table_f2_width int, 
  p_table_f1_range bigint, 
  p_schema int, 
  p_additional_run_nr int, 
  p_run_tag text default 'insert',
  p_create_method text default 'unnest', 
  p_rows_per_message bigint default 0  )
language plpgsql as $$
declare
  array_id bigint[];
  array_f1 bigint[];
  array_f2 text[];
  array_f3 text[];
  array_f4 text[];
  array_f5 text[];
  array_f6 text[];
  array_f7 text[];
  array_f8 text[];
  array_f9 text[];
  array_f10 text[];
  v_clock_batch timestamp;
  v_clock_begin timestamp := clock_timestamp();
  v_clock_end timestamp;
  v_start_id int := p_rows * p_additional_run_nr;
  v_end_id int := v_start_id + p_rows - 1;
begin
  raise notice 'inserting % rows (id % to %) into schema ybio%', v_end_id-v_start_id, v_start_id, v_end_id, p_schema;
  raise notice 'rows per commit: %', p_create_rows_per_commit;
  v_clock_batch := clock_timestamp();

  if p_create_method = 'unnest' then

    raise notice 'create method: %. This means all % rows are inserted in a single command and committed.', p_create_method, v_end_id-v_start_id;

    /*
     * if p_rows_per_message = 0, which is the default, then set p_rows_per_message to p_create_rows_per_commit.
     * if p_rows_per_message > 0, round it up to the next p_rows_per_message multiple.
     * it doesn't make sense to report progress of the array creation.
     */
    if p_rows_per_message = 0 then
      p_rows_per_message := p_create_rows_per_commit;
    else
      p_rows_per_message := (((p_rows_per_message/p_create_rows_per_commit)+1)*p_create_rows_per_commit);
    end if;
    raise notice 'progress is reported per % rows.', p_rows_per_message;

    /*
     * this is the main loop of the unnest batch insert method.
     */
    for v_counter in v_start_id..v_end_id loop

      /*
       * build the array
       */
      array_id[v_counter] := v_counter;
      array_f1[v_counter] := dbms_random.value(1,p_table_f1_range);
      array_f2[v_counter] := dbms_random.string('a',p_table_f2_width);
      array_f3[v_counter] := dbms_random.string('a',p_table_f2_width);
      array_f4[v_counter] := dbms_random.string('a',p_table_f2_width);
      array_f5[v_counter] := dbms_random.string('a',p_table_f2_width);
      array_f6[v_counter] := dbms_random.string('a',p_table_f2_width);
      array_f7[v_counter] := dbms_random.string('a',p_table_f2_width);
      array_f8[v_counter] := dbms_random.string('a',p_table_f2_width);
      array_f9[v_counter] := dbms_random.string('a',p_table_f2_width);
      array_f10[v_counter] := dbms_random.string('a',p_table_f2_width);

      /*
       * insert the rows using the array, commit and then empty the array.
       */
      if mod(v_counter, p_create_rows_per_commit) = 0 then
        insert into benchmark_table (id, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10)
          select unnest(array_id), unnest(array_f1), unnest(array_f2), unnest(array_f5), unnest(array_f4), unnest(array_f5), unnest(array_f6), unnest(array_f7), unnest(array_f8), unnest(array_f9), unnest(array_f10);
        array_id := '{}';
        array_f1 := '{}';
        array_f2 := '{}';
        array_f3 := '{}';
        array_f4 := '{}';
        array_f5 := '{}';
        array_f6 := '{}';
        array_f7 := '{}';
        array_f8 := '{}';
        array_f9 := '{}';
        array_f10 := '{}';
        commit;
      end if;

      /*
       * report the progress.
       */
      if mod(v_counter, p_rows_per_message) = 0 and v_counter != 0 then
        raise notice 'progress: % rows, %; % rows/s avg lat % s',
          to_char(v_counter-v_start_id,'999G999G999G999'),
          to_char((100*(v_counter-v_start_id::float)/(v_end_id-v_start_id)),'999.99')||' %',
          to_char(p_rows_per_message/extract(epoch from clock_timestamp()-v_clock_batch),'999G999'),
          to_char(extract(epoch from clock_timestamp()-v_clock_batch)/p_rows_per_message,'9999.999G999');
        v_clock_batch := clock_timestamp();
      end if;

    end loop;

    /*
     * if there is a leftover in the array, insert it and commit.
     */
    if array_length(array_id,1) > 0 then
      insert into benchmark_table (id, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10)
        select unnest(array_id), unnest(array_f1), unnest(array_f2), unnest(array_f5), unnest(array_f4), unnest(array_f5), unnest(array_f6), unnest(array_f7), unnest(array_f8), unnest(array_f9), unnest(array_f10);
      commit;
    end if;

  elsif p_create_method = 'row' then

    raise notice 'create method: %. This means all % rows are inserted using single row inserts.', p_create_method, v_end_id-v_start_id;

    if p_rows_per_message = 0 then
      p_rows_per_message := p_create_rows_per_commit;
    end if;
    raise notice 'progress is reported per % rows.', p_rows_per_message;

    /*
     * this is the main loop of the row insert method.
     */
    for v_counter in v_start_id..v_end_id loop

      /*
       * the insert command.
       */
      insert into benchmark_table (id, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10) 
        values (v_counter, 
                dbms_random.value(1,p_table_f1_range), 
                dbms_random.string('a',p_table_f2_width), 
                dbms_random.string('a',p_table_f2_width), 
                dbms_random.string('a',p_table_f2_width), 
                dbms_random.string('a',p_table_f2_width), 
                dbms_random.string('a',p_table_f2_width), 
                dbms_random.string('a',p_table_f2_width), 
                dbms_random.string('a',p_table_f2_width), 
                dbms_random.string('a',p_table_f2_width), 
                dbms_random.string('a',p_table_f2_width)
               );

      /*
       * commit.
       */
      if mod(v_counter, p_create_rows_per_commit) = 0 then
        commit;
      end if;

      /*
       * report the progress.
       */
      if mod(v_counter, p_rows_per_message) = 0 and v_counter != 0 then
        raise notice '% rows, %; % rows/s avg lat % s/row',
          to_char(v_counter-v_start_id,'999G999G999G999'),
          to_char((100*(v_counter-v_start_id::float)/(v_end_id-v_start_id)),'999.99')||' %',
          to_char(p_rows_per_message/extract(epoch from clock_timestamp()-v_clock_batch),'999G999'),
          to_char(extract(epoch from clock_timestamp()-v_clock_batch)/p_rows_per_message,'9999.999G999');
        v_clock_batch := clock_timestamp();
      end if;

    end loop;

    /*
     * commit leftover rows.
     */
    commit;

  else

    /*
     * this should not be possibe.
     */
    raise exception 'create_method % should be unnest or row', p_create_method;
    
  end if;

  /*
   * end of run summary.
   */
  v_clock_end := clock_timestamp();
  raise notice 'done inserting % rows (id % to %) into schema ybio%', 
    v_end_id-v_start_id, 
    v_start_id, 
    v_end_id, 
    p_schema;
  raise notice 'method: %, rows per commit: %, total time: %', 
    p_create_method, 
    p_create_rows_per_commit, 
    to_char(extract(epoch from clock_timestamp()-v_clock_begin))::interval;
  raise notice '% rows, %; % rows/s avg lat % s/row',
    to_char(v_end_id-v_start_id,'999G999G999G999'),
    to_char((100*(v_end_id-v_start_id::float)/(v_end_id-v_start_id)),'999.99')||' %',
    to_char((v_end_id-v_start_id)/extract(epoch from clock_timestamp()-v_clock_begin),'999G999'),
    to_char(extract(epoch from clock_timestamp()-v_clock_begin)/(v_end_id-v_start_id),'9999.999G999');

  /*
   * insert into ybio.results table
   */
  insert into ybio.results 
    (config_id, start_time, end_ttime, inet_server_addrs, pg_backend_pid, nr_total, nr_insert, nr_select, nr_update, nr_delete, nr_notfound, run_tag )
    values
    (p_config_id, v_clock_begin, v_clock_end, inet_server_addrs(), pg_backed_pid(), v_end_id-v_start_id, v_end_id-v_start_id, 0, 0, 0, 0, p_run_tag );
  
end $$;
