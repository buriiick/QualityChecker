SELECT
c.column_name
FROM columns c
LEFT JOIN primary_keys pk
ON pk.column_name = c.column_name
	AND pk.table_name = c.table_name
    AND pk.table_schema = c.table_schema
WHERE c.table_name = '{table}'
    AND c.table_schema = '{schema_name}'
    AND pk.constraint_id IS NULL
    AND c.column_name NOT IN ('tech_load_ts', 'tech_job_id', 'tech_is_deleted')
ORDER BY c.ordinal_position;