create or replace procedure ybio.setup ( p_config_id int, p_perform_schema_nr int default 0, p_run_tag text default 'insert', p_additional_run_nr int default 0 )
language plpgsql as $$
declare
  v_rows bigint;
  v_create_rows_per_commit bigint;
  v_create_rows_per_message bigint;
  v_create_method text;
  v_number_schemas int;
  v_drop_before_insert boolean;
  v_table_primary_key boolean;
  v_table_primary_key_type text;
  v_table_tablets int;
  v_table_f2_width int;
  v_table_f1_range bigint;
  v_index_f1 boolean;
  v_index_f1_type text;
  v_index_f1_tablets int;
begin

  /*
   * read the configuration form the ybio.config table.
   * raise exception and stop if not found.
   */
  select rows,
         rows_per_message,
         number_schemas,
         drop_before_insert,
         create_rows_per_commit,
         create_method,
         table_primary_key,
         table_primary_key_type,
         table_tablets,
         table_f2_width,
         table_f1_range,
         index_f1,
         index_f1_type,
         index_f1_tablets
  into   v_rows,
         v_create_rows_per_message,
         v_number_schemas,
         v_drop_before_insert,
         v_create_rows_per_commit,
         v_create_method,
         v_table_primary_key,
         v_table_primary_key_type,
         v_table_tablets,
         v_table_f2_width,
         v_table_f1_range,
         v_index_f1,
         v_index_f1_type,
         v_index_f1_tablets
  from   ybio.config
  where  id = p_config_id;
  if not found then
    raise exception 'config id % not found in ybio.config table', config_id;
  end if;

  /*
   * perform creating the schema(s) and creating the table benchmark_table in it.
   * there are two options: 
   * p_perform_schema_nr = 0: 1 to v_number_schemas schemas are created serially (default)
   * p_perform_schema_nr > 0: this schema number only is created.
   */
  if p_perform_schema_nr = 0 then

    for v_schema_nr in 1..v_number_schemas loop

      /*
       * p_additional_run_nr makes it possible to perform an additional run of inserts into the same 
       * schema into the same table. 
       * Allowed values are:
       * p_additional_run_nr = 0: drop and create schema and load.
       * p_additional_run_nr > 0: just perform the insert.
       *
       * also, when drop_before_insert is set to false, we skip dropping and creating the table.
       * this makes it possible to define your own table.
       */
      if p_additional_run_nr = 0 or not v_drop_before_insert then

        /*
         * drop and create schema, then set search_path to the schema.
         */
        execute format('drop schema if exists ybio%s cascade', v_schema_nr);
        execute format('create schema ybio%s', v_schema_nr);
        execute format('set search_path to ybio%s', v_schema_nr);

        /* 
         * create the table and optionally the index.
         */
        execute format('create table benchmark_table ( id bigint, f1 bigint, f2 text, f3 text, f4 text, f5 text, f6 text, f7 text, f8 text, f9 text, f10 text %s ) %s',
          case v_table_primary_key when true then format(', primary key ( id %s)', v_table_primary_key_type) else '' end,
          case v_table_tablets when 0 then '' else format('split into %s tablets', v_table_tablets) end
        );

        if v_index_f1 then
           execute format('create index benchmark_table_i_f1 on benchmark_table( f1 %s ) %s',
             v_index_f1_type,
             case v_index_f1_tablets when 0 then '' else format('split into %s tablets', v_index_f1_tablets) end
           );
        end if;

      else
       
        /*
         * p_additional_run_nr is > 0: just set search_path.
         */
        execute format('set search_path to ybio%s', v_schema_nr);

      end if;

      /*
       * call the ybio.insert procedure to perform the inserts.
       */
      call ybio.insert(p_config_id, v_rows, v_create_rows_per_commit, v_table_f2_width, v_table_f1_range, v_schema_nr, p_additional_run_nr, p_run_tag, v_create_method, v_create_rows_per_message );

    end loop;

  elsif p_perform_schema_nr > 0 then

    /*
     * p_additional_run_nr makes it possible to perform an additional run of inserts into the same 
     * schema into the same table. 
     * Allowed values are:
     * p_additional_run_nr = 0: drop and create schema and load.
     * p_additional_run_nr > 0: just perform the insert.
     *
     * also, when drop_before_insert is set to false, we skip dropping and creating the table.
     * this makes it possible to define your own table.
     */
    if p_additional_run_nr = 0 or not v_drop_before_insert then

      /*
       * p_perform_schema_nr allows specifying a single schema number to be done.
       * However, it must be within the bounds of the number of schemas set by v_number_schemas from the ybio.config table.
       */
      if p_perform_schema_nr > v_number_schemas then

        raise exception 'schema nr % too high, maximal number of config % is %', p_perform_schema_nr, p_config_id, v_number_schemas;

      end if;

      /*
       * drop and create schema, then set search_path to the schema.
       */
      execute format('drop schema if exists ybio%s cascade', p_perform_schema_nr);
      execute format('create schema ybio%s', p_perform_schema_nr);
      execute format('set search_path to ybio%s', p_perform_schema_nr);

      /*
       * create the table and optionally the index.
       */
      execute format('create table benchmark_table ( id bigint, f1 bigint, f2 text, f3 text, f4 text, f5 text, f6 text, f7 text, f8 text, f9 text, f10 text %s ) %s',
        case v_table_primary_key when true then format(', primary key ( id %s)', v_table_primary_key_type) else '' end,
        case v_table_tablets when 0 then '' else format('split into %s tablets', v_table_tablets) end
      );

      if v_index_f1 then
        execute format('create index benchmark_table_i_f1 on benchmark_table( f1 %s ) %s',
          v_index_f1_type,
          case v_index_f1_tablets when 0 then '' else format('split into %s tablets', v_index_f1_tablets) end
        );
      end if;

    else

      /*
       * p_additional_run_nr is > 0: just set search_path.
       */
      execute format('set search_path to ybio%s', p_perform_schema_nr);

    end if;

    /*
     * call the ybio.insert procedure to perform the inserts.
     */
    call ybio.insert(p_config_id, v_rows, v_create_rows_per_commit, v_table_f2_width, v_table_f1_range, p_perform_schema_nr, p_additional_run_nr, p_run_tag, v_create_method, v_create_rows_per_message );

  else

    /*
     * not sure how you could get here, but for completeness sake
     */
    raise exception 'schema nr % cannot be used', p_perform_schema_nr;

  end if;

end $$;
