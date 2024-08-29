SELECT 
    MAX(COALESCE(TO_CHAR(DATE(tech_load_ts), 'YYYY-MM-DD'), '1999-01-01'))
FROM {schema}.{table};