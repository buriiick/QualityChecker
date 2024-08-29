SELECT
    1
FROM {schema}.{table}
WHERE convert_to({column}::text, 'UTF8')::text <> {column}::text
LIMIT 1;