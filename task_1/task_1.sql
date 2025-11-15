with departure as (
	select
		f.aircraft_id
	,	avg(f.actual_departure - f.scheduled_departure) avg_delay_dep
	from public.flights f
	where f.scheduled_departure < f.actual_departure
	group by f.aircraft_id
)
, arrival as (
	select
		f.aircraft_id
	,	avg(f.actual_arrival - f.scheduled_arrival) avg_delay_arr
	from public.flights f
	where f.scheduled_arrival < f.actual_arrival
	group by f.aircraft_id
)
, part as (
	select
		f.aircraft_id
	,	count(*) filter (where f.actual_departure - f.scheduled_departure > interval '15 min')::numeric / count(*) part
	from public.flights f
	group by f.aircraft_id
)
, all_aircraft as (
	select f.aircraft_id
	from public.flights f
	group by f.aircraft_id
)
select
	aa.aircraft_id
,	coalesce(d.avg_delay_dep, interval '0 min') avg_delay_dep
,	coalesce(a.avg_delay_arr, interval '0 min') avg_delay_arr
,	p.part
from all_aircraft aa
left join departure d on aa.aircraft_id = d.aircraft_id
left join arrival a on aa.aircraft_id = a.aircraft_id
left join part p on aa.aircraft_id = p.aircraft_id
;
-- проверить корректность результата можно с использованием сгенерированного тестовом датасета