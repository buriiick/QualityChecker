SELECT
    c.column_name
FROM
    information_schema.columns c
LEFT JOIN
    information_schema.key_column_usage pk
ON
    pk.column_name = c.column_name
    AND pk.table_name = c.table_name
    AND pk.table_schema = c.table_schema
WHERE
    c.table_name = '{table}'
    AND c.table_schema = '{schema_name}'
    AND pk.constraint_name IS NULL
    AND c.column_name NOT IN ('tech_load_ts', 'tech_job_id', 'tech_is_deleted')
ORDER BY
    c.ordinal_position;