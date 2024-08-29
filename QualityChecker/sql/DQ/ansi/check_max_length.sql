SELECT 
    (SELECT character_maximum_length
     FROM information_schema.columns
     WHERE 
         data_type LIKE '%char%'
         AND table_schema = '{schema}'
         AND table_name = '{table}'
         AND column_name = '{column}'
    ) <=
    (SELECT MAX(CHAR_LENGTH({column})) 
     FROM {schema}.{table})