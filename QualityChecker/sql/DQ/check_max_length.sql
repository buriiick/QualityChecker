select (select character_maximum_length
from columns
where 
data_type ilike '%char%'
and table_schema ilike '{schema}'
and table_name ilike '{table}'
and column_name ilike '{column}') <=
       (select max(bit_length({column}))/8 from {schema}.{table})