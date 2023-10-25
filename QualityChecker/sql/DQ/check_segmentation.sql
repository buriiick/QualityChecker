SELECT LISTAGG(percent) 
FROM
(SELECT DISTINCT LEFT(TO_CHAR(cnt/(SELECT count(1) as all_cnt FROM {schema}.{table})*100),2)|| '% 'as percent
 FROM (
	SELECT MOD(HASH({pk}),
		  (select count(1) from nodes)) as node,count(1) as cnt
	FROM {schema}.{table}
	group by 1
	   ) a
) q;