SELECT c.column_name 
FROM columns c
WHERE 
{where_clause}
and 
c.table_name ilike '{table}'
AND c.table_schema ilike '{schema_name}'
ORDER BY c.ordinal_position;