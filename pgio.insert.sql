create or replace procedure pgio.insert( v_rows bigint, v_create_batch_size bigint, v_table_f2_width int, v_table_f1_range bigint, v_schema int )
language plpgsql as $$
declare
  i_id bigint[];
  i_f1 bigint[];
  i_f2 text[];
  clock_batch timestamp;
  clock_begin timestamp := clock_timestamp();
begin
  raise notice 'inserting % rows into schema pgio% with batchsize %', v_rows, v_schema, v_create_batch_size;
  clock_batch := clock_timestamp();
  for i in 1..v_rows loop
    i_id[i] := i;
    i_f1[i] := dbms_random.value(1,v_table_f1_range);
    i_f2[i] := dbms_random.string('a',v_table_f2_width);
    if mod(i,v_create_batch_size) = 0 then
      insert into benchmark_table (id, f1, f2)
      select unnest(i_id), unnest(i_f1), unnest(i_f2);
      i_id := '{}';
      i_f1 := '{}';
      i_f2 := '{}';
      commit;
      raise notice 'progress: % rows, %, % rows/second', i, to_char((100*(i::float)/v_rows),'999.99')||'%', to_char(v_create_batch_size/extract(epoch from clock_timestamp()-clock_batch),'999999');
      clock_batch := clock_timestamp();
    end if;
  end loop;
  if array_length(i_id,1) > 0 then
    insert into benchmark_table (id, f1, f2)
    select unnest(i_id), unnest(i_f1), unnest(i_f2);
    commit;
  end if; 
  raise notice 'done inserting into pgio%, % rows, % rows/second', v_schema, v_rows, to_char(v_rows/extract(epoch from clock_timestamp()-clock_begin),'999999');
end $$;