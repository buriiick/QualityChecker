
--select count(1)
--from (
SELECT
1
--count({column})
FROM {schema}.{table}
WHERE nvl(to_char({column}),'') <> ''
limit 1
--) a;