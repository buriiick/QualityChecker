SELECT
    column_name
FROM 
    information_schema.columns 
WHERE 
    UPPER(table_name) = UPPER('{table}')
    AND UPPER(table_schema) = UPPER('{schema_name}')
    --AND column_name NOT IN ('tech_load_ts', 'tech_job_id', 'tech_is_deleted')
ORDER BY 
    ordinal_position;