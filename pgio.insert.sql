create or replace procedure pgio.insert( p_rows bigint, p_create_rows_per_commit bigint, p_table_f2_width int, p_table_f1_range bigint, p_schema int, p_additional_run_nr int, p_create_method text default 'unnest', p_rows_per_message bigint default 0  )
language plpgsql as $$
declare
  array_id bigint[];
  array_f1 bigint[];
  array_f2 text[];
  v_clock_batch timestamp;
  v_clock_begin timestamp := clock_timestamp();
  v_start_id int := p_rows * p_additional_run_nr;
  v_end_id int := v_start_id + p_rows - 1;
begin
  raise notice 'inserting % rows (id % to %) into schema pgio%', v_end_id-v_start_id, v_start_id, v_end_id, p_schema;
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

      /*
       * insert the rows using the array, commit and then empty the array.
       */
      if mod(v_counter, p_create_rows_per_commit) = 0 then
        insert into benchmark_table (id, f1, f2)
          select unnest(array_id), unnest(array_f1), unnest(array_f2);
        array_id := '{}';
        array_f1 := '{}';
        array_f2 := '{}';
        commit;
      end if;

      /*
       * report the progress.
       */
      if mod(v_counter, p_rows_per_message) = 0 and v_counter != 0 then
        raise notice 'progress: % rows, %, % rows/second', 
          v_counter-v_start_id, 
          to_char((100*(v_counter-v_start_id::float)/(v_end_id-v_start_id)),'999.99')||'%', 
          to_char(p_rows_per_message/extract(epoch from clock_timestamp()-v_clock_batch),'999999'
        );
        v_clock_batch := clock_timestamp();
      end if;

    end loop;

    /*
     * if there is a leftover in the array, insert it and commit.
     */
    if array_length(array_id,1) > 0 then
      insert into benchmark_table (id, f1, f2)
        select unnest(array_id), unnest(array_f1), unnest(array_f2);
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
      insert into benchmark_table (id, f1, f2) values (v_counter, dbms_random.value(1,p_table_f1_range), dbms_random.string('a',p_table_f2_width));

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
        raise notice 'progress: % rows, %, % rows/second', 
          v_counter-v_start_id, 
          to_char((100*(v_counter-v_start_id::float)/(v_end_id-v_start_id)),'999.99')||'%', 
          to_char(p_rows_per_message/extract(epoch from clock_timestamp()-v_clock_batch),'999999'
        );
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
   */-
  raise notice 'done inserting % rows (id % to %) into schema pgio%', v_end_id-v_start_id, v_start_id, v_end_id, p_schema;
  raise notice 'total time: % seconds, average number of rows per second: %', round(extract(epoch from clock_timestamp()-v_clock_begin)::numeric,2), to_char((v_end_id-v_start_id)/extract(epoch from clock_timestamp()-v_clock_begin),'999999');

end $$;
