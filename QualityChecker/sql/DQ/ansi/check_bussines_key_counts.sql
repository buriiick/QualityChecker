SELECT COUNT(1)
FROM (
    SELECT {pk}
    FROM {schema}.{table}
    GROUP BY {pk}
) q;