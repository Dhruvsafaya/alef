-- A part of falcon quality alerts https://alefeducation.atlassian.net/browse/ALEF-68280
-- Note: Clean duplicates from fact weekly goal when same records arrive in same batch

delete from alefdw.fact_weekly_goal
where fwg_dw_id in
(SELECT a.fwg_dw_id
FROM (
  SELECT *,
         row_number() over (partition by fwg_class_dw_id,
         fwg_student_dw_id,
         fwg_type_dw_id,
         fwg_id,
         fwg_action_status,
         fwg_dw_created_time
         order by
         fwg_created_time desc) as rnk
  FROM alefdw.fact_weekly_goal) a
WHERE a.rnk > 1);
