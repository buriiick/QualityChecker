--Простой
select a.table_schema,a.table_name
from tables a
where a.table_schema ilike 'ODS_GRP%'
-- and a.table_name in ('GOODS', 'CITIES')
--and a.table_name in ('GOODS')
order by
a.table_schema desc,a.table_name;



--сложный
/*select a.table_schema,a.table_name
from tables a
join
(
select count(1) cnt,
 a.table_schema,a.table_name
from tables a
join tables b
on a.table_name = b.table_name
   and a.table_schema = replace(b.table_schema,'STG_','ODS_')
group by a.table_schema,a.table_name
having cnt = 2
       and a.table_schema ilike 'ODS_GRP%'
) b
on a.table_schema = b.table_schema
   and a.table_name = b.table_name
where
create_time  >= '2023-01-01'
--and create_time  < '2023-03-01'
and a.table_schema ilike 'ODS_GRP%'
and a.table_name not ilike'%DELETE%'
and a.table_name not ilike'%BACKUP%'
--and a.table_name ilike '%GOODS%'
order by
a.table_schema desc,a.table_name
limit 4;*/