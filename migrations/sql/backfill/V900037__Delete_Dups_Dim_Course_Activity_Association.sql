--ALEF-71582 backup table name - backups.dim_course_activity_association_2025_03_20
delete
from alefdw.dim_course_activity_association
where caa_dw_id in
      (select a.caa_dw_id
       from (select *,
                    dense_rank() over (partition by caa_course_id, caa_activity_id, caa_grade order by caa_dw_id) as rnk
             from alefdw.dim_course_activity_association
             WHERE caa_status = 1
               and caa_attach_status = 1) a
       where a.rnk > 1);