DELETE
FROM alefdw.fact_lesson_feedback
where lesson_feedback_staging_id in
(select lesson_feedback_staging_id
from (SELECT t2.*,
             ROW_NUMBER()
             OVER (PARTITION BY t2.lesson_feedback_id ORDER BY t2.lesson_feedback_fle_ls_dw_id) AS rnk
FROM alefdw.fact_lesson_feedback t2
where lesson_feedback_fle_ls_dw_id is not null
) lf
where lf.rnk > 1);