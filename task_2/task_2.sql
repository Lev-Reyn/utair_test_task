-- решение подходит в том случае если есть данные за каждую минуту полёта, иначе потребуется
-- предварительная замена значений (например средние значения)

-- c оконками
--explain analyze
select
	t1.flight_id
,	t1.max_altitude
,	t1.avg_speed
,	t2."timestamp" -- при прод реализации уточню у заказчиков необходимо ли замена null значений
from
	(select
		ft.flight_id
	,	max(ft.altitude) max_altitude
	,	round(avg(ft.speed), 2) avg_speed
	from public.flight_telemetry ft
	group by ft.flight_id) t1
left join
	(select
		ft.flight_id
	,	ft.timestamp
	,	row_number() over(partition by ft.flight_id order by ft."timestamp") rn
	from public.flight_telemetry ft
	where ft.altitude > 1000) t2
on t1.flight_id = t2.flight_id
and t2.rn = 1
;

-- без оконок
--explain analyze
select
		ft.flight_id
	,	max(ft.altitude) max_altitude
	,	round(avg(ft.speed), 2) avg_speed
	,	min(case when ft.altitude > 1000 then ft."timestamp" end) "timestamp"
from public.flight_telemetry ft
group by ft.flight_id


-- чем удобно с оконками:
--1) метрики и событие отдельно считаю, добавлять добавлять доп. метрики и другие события удобно
--2) row_number стандартная оконка которая есть в большинстве субд

-- чем удобно без оконок:
--1) использую sql стандартный для больниства субд
--2) стоимость запроса ниже