
SELECT
1
FROM {schema}.{table}
WHERE nvl(to_char({column}),'') <> ''
limit 1