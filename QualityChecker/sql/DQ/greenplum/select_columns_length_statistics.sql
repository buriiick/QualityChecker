WITH column_length AS (
    SELECT character_maximum_length
    FROM information_schema.columns
    WHERE 
        data_type ILIKE '%char%' AND
        table_schema ILIKE '{schema}' AND
        table_name ILIKE '{table}' AND
        column_name ILIKE '{column}'
),
max_length AS (
    SELECT MAX(LENGTH({column})) AS max_length 
    FROM {schema}.{table}
)
SELECT 
    'Varchar(' || max_length::int || ') / (' || character_maximum_length || ')' 
FROM 
    max_length, column_length;