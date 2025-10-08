delete
from alefdw.dim_teacher_test_item_association
where ttia_dw_id in
      (select a.ttia_dw_id
       from (select *,
                    dense_rank() over (partition by ttia_test_id, ttia_test_item_id order by ttia_dw_id) as rnk
             from alefdw.dim_teacher_test_item_association
             WHERE ttia_status = 1
            ) a
       where a.rnk > 1);