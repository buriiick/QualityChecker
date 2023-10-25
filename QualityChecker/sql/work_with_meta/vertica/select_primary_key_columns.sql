SELECT
column_name
FROM primary_keys 
    WHERE table_name ilike '{table}'
	AND table_schema ilike '{schema_name}'
    --AND column_name NOT IN ('tech_load_ts', 'tech_job_id', 'tech_is_deleted')
ORDER BY ordinal_position;
