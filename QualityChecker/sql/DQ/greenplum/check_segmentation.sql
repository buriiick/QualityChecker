SELECT string_agg(percent, ', ')
FROM (
    SELECT DISTINCT 
           TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM TO_CHAR(cnt * 100.0 / (SELECT COUNT(1) FROM {schema}.{table}), 'FM90.99'))) || '%' as percent
    FROM (
        SELECT gp_segment_id AS segment, COUNT(1) as cnt
        FROM {schema}.{table}
        GROUP BY gp_segment_id
    ) a
) q;