SELECT column_name 
FROM information_schema.columns 
WHERE 
{where_clause}
AND table_name ILIKE '{table}' 
AND table_schema ILIKE '{schema_name}' 
ORDER BY ordinal_position;