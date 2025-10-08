-- backups.dim_teacher_test_candidate_association_2025_02_11
delete
from alefdw.dim_teacher_test_candidate_association
where ttca_dw_id in
      (select a.ttca_dw_id
       from (select *,
                    dense_rank() over (partition by ttca_test_delivery_id,ttca_test_candidate_id order by ttca_dw_id) as rnk
             from alefdw.dim_teacher_test_candidate_association
             where ttca_status = 1) a
       where a.rnk > 1);