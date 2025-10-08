DELETE
FROM alefdw.fact_experience_submitted
where fes_dw_id in
(select fes_dw_id
from (SELECT t2.fes_dw_id,
             ROW_NUMBER()
             OVER (PARTITION BY t2.fes_id, t2.exp_uuid, t2.fes_ls_id ORDER BY t2.fes_exp_dw_id) AS rnk
FROM alefdw.fact_experience_submitted t2
where t2.fes_exp_dw_id is not null and t2.fes_ls_dw_id is not null
) lf
where lf.rnk > 1);
