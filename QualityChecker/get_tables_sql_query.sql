select a.table_schema ,a.table_name from tables a
join  (select concat(replace(table_schema,'STG_','ODS_'),table_name) sche,count(1)
           from tables
           --where table_schema ilike '%_GARANTIK%'
           group by concat(replace(table_schema,'STG_','ODS_'),table_name)
           having count(1) = 2) b
           on concat(a.table_schema,a.table_name) = b.sche
and table_schema ilike 'ODS_%SOVA%'
--and table_name in ('UZBEKISTAN','KAZAKHSTAN_MIP','KAZAKHSTAN_TC','EAEU')
and table_name ilike '%report%'
order by 
a.table_schema desc,a.table_name;