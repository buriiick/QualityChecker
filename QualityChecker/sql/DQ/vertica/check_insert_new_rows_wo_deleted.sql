--В дев некорректно материализуются CTE. Чтобы скрипт работал в дев дуюлирую CTE в скрипте
with stg1 as (
select * from {stg_schema}.{table}
LIMIT 1 over(PARTITION BY {pk_columns_str} order by tech_load_ts desc)
),
stg2 as (
select * from {stg_schema}.{table}
LIMIT 1 over(PARTITION BY {pk_columns_str} order by tech_load_ts desc)
),
stg3 as (
select * from {stg_schema}.{table}
LIMIT 1 over(PARTITION BY {pk_columns_str} order by tech_load_ts desc)
),
ods1 as (
select * from {ods_schema}.{table}
where tech_load_ts < (select min(tech_load_ts) from {stg_schema}.{table})
LIMIT 1 over(PARTITION BY {pk_columns_str} order by tech_load_ts desc)
),
ods2 as (
select * from {ods_schema}.{table}
where tech_load_ts < (select min(tech_load_ts) from {stg_schema}.{table})
LIMIT 1 over(PARTITION BY {pk_columns_str} order by tech_load_ts desc)
)
,
ods3 as (
select * from {ods_schema}.{table}
where tech_load_ts < (select min(tech_load_ts) from {stg_schema}.{table})
LIMIT 1 over(PARTITION BY {pk_columns_str} order by tech_load_ts desc)
)
,
ods_actual as (
select 1 from {ods_schema}.{table}
where tech_load_ts >= (select max(tech_load_ts) from {stg_schema}.{table})
)
,
insert_count as (
select count(1) I from stg1 as stg
left join ods1 as ods
on {pk_join} 
where ods.tech_load_ts is null 
),
update_count as (
select count(1) U from stg2 as stg 
inner join ods2 as ods 
on {pk_join} 
WHERE ( {u_compare} ) 
)




select sum(u) = 0 from (
select u from update_count 
union all
select i from insert_count
union all 
select -count(1) from ods_actual) q


