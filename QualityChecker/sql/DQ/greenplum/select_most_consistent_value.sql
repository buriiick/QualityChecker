WITH cte AS (
    SELECT COUNT(1) AS cnt,
           {column} AS col
    FROM {schema}.{table}
    GROUP BY {column}
    ORDER BY cnt DESC
    LIMIT 1
)
SELECT '''' || COALESCE(col::text, 'NULL') || ''' ' || cnt || ' / ' || 
       (SELECT COUNT(1) 
        FROM {schema}.{table}) || ' (' || 
       LEFT((cnt * 100.0 / (SELECT COUNT(1) 
                                   FROM {schema}.{table}), 'FM999.99')::text, 5) || ' % )'
FROM cte;