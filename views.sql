create view ybio.results_overview as
select 
run_tag "tag",
count(*) "tot_nr",
avg(extract( epoch from (end_time-start_time))) "avg_run_time_sec",
sum(nr_total) "total_rows",
sum(nr_total)/avg(extract( epoch from (end_time-start_time))) "avg_tot_rows_per_sec",
avg(extract( epoch from (end_time-start_time)))/(sum(nr_total)/count(*)) "avg_lat_tot_rows",
sum(nr_insert) "rows_insert",
sum(nr_select) "rows_select",
sum(nr_update) "rows_update",
sum(nr_delete) "rows_delete",
sum(nr_notfound) "rows_notfound"
from ybio.results
group by run_tag
order by run_tag;
