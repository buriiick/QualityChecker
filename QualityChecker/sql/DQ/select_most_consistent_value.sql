with cte as
    ( select  count(1) cnt,
             {column} as col
        from {schema}.{table}
        group by {column} order by 1 desc limit 1  )
    select ''''|| nvl(to_char(col),'NULL') || ''' ' || cnt || ' из ' || (select count(1)
	from  {schema}.{table}) || ' ('
           || left(to_char(cnt * 100 / (select count(1) from {schema}.{table}) ),5) || ' % )'
from cte;