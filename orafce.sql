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