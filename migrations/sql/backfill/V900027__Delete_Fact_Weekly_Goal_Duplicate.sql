DELETE
FROM alefdw.fact_weekly_goal
where fwg_dw_id in
(SELECT fwg_dw_id
from (SELECT *,
             row_number() over (partition by fwg_created_time,
             fwg_dw_created_time,
             fwg_id,
             fwg_action_status,
             fwg_type_dw_id,
             fwg_student_dw_id,
             fwg_tenant_dw_id,
             fwg_class_dw_id,
             fwg_star_earned
             order by
             fwg_dw_id) as rnk
FROM alefdw.fact_weekly_goal) a
WHERE a.rnk > 1);