select 
max(nvl(to_char(date(tech_load_ts)),'1999-01-01'))
from {schema}.{table};