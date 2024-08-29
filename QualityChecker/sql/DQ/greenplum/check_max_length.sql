SELECT 
    (SELECT character_maximum_length
     FROM information_schema.columns
     WHERE 
         data_type ILIKE '%char%' AND
         table_schema = '{schema}' AND
         table_name = '{table}' AND
         column_name = '{column}') <=
    (SELECT MAX(LENGTH({column})) 
     FROM {schema}.{table});