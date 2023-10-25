with column_length as
(select character_maximum_length
from columns
where 
data_type ilike '%char%'
and table_schema ilike '{schema}'
and table_name ilike '{table}'
and column_name ilike '{column}'),
max_length as (select max(bit_length({column}))/8  max_length from {schema}.{table})

select 'Varchar('||max_length::int || ') из (' || character_maximum_length ||')' from max_length,column_length