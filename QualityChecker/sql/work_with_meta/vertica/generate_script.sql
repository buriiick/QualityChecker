INSERT INTO {ods_schema_name}.{table} ({all_columns_insert})
WITH 
ods AS
(SELECT * FROM {ods_schema_name}.{table} 
		LIMIT 1 OVER( PARTITION BY {pk_columns_str} 
		ORDER BY tech_load_ts DESC )), 

stg AS
(SELECT * FROM {stg_schema_name}.{table} 
		LIMIT 1 OVER( PARTITION BY {pk_columns_str} 
		ORDER BY tech_load_ts DESC ))
SELECT {all_columns_str}
FROM stg
LEFT JOIN ods
ON {pk_join}
WHERE {columns_compare};