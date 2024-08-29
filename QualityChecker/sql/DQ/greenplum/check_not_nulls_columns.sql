SELECT
1
FROM {schema}.{table}
WHERE COALESCE({column}::text, '') <> ''
LIMIT 1;