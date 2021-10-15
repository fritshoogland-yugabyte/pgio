do $$
declare
  orafce_available int;
  orafce_installed int;
begin
  select count(*) into orafce_available from pg_available_extensions where name = 'orafce';
  if orafce_available < 1 then
    raise exception 'orafce extension not available';
  end if;
  select count(*) into orafce_installed from pg_extension where extname = 'orafce';
  if orafce_installed < 1 then
    execute 'create extension orafce';
  end if;
end $$;
drop schema if exists pgio cascade;
create schema pgio;
create table pgio.config (
  id 				serial primary key,
  rows 				bigint default 1000000,
  create_batch_size 		bigint default 1000,
  number_schemas		int default 1,
  table_primary_key		boolean default true,
  table_primary_key_type	text default 'hash',
  table_tablets			int default 0,
  table_f2_width		int default 100,
  table_f1_range		bigint default 1000000,
  index_f1			boolean default false,
  index_f1_type			text default 'hash',
  index_f1_tablets		int default 0,
  run_batch_size		bigint default 1000,
  update_pct			int default 0,
  delete_pct			int default 0
);
create table pgio.results (
  run_id		serial,
  start_time		timestamp,
  end_time		timestamp
);
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
    call pgio.insert(v_rows, v_create_batch_size, v_table_f2_width, v_table_f1_range, schema_nr );
  end loop;
end $$;
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
