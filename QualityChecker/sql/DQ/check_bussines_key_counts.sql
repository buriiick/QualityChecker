select count(1)
from (
select {pk}
from {schema}.{table}
group by {pk}
) q