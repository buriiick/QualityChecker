SELECT
1
FROM {schema}.{table}
WHERE makeutf8(to_char({column})) <> to_char({column})
limit 1;