--ALEF-67974 DELETE dups caused due reprocessing dup data due to api timestamp precision restriction
create table backups.fact_jira_issue_ALEF_67974
as
select *
FROM alefdw.fact_jira_issue
where fji_dw_id in
(SELECT fji_dw_id
from (SELECT *,
             row_number() over (partition by fji_key,
             fji_created_time
             order by
             fji_dw_created_time) as rnk
FROM alefdw.fact_jira_issue) a
WHERE a.rnk > 1);

delete
FROM alefdw.fact_jira_issue
where fji_dw_id in (SELECT fji_dw_id from backups.fact_jira_issue_ALEF_67974);
