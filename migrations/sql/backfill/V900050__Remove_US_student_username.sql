update alefdw.dim_student
set student_username = null
from alefdw.dim_student st
    join alefdw.dim_school s on st.student_school_dw_id = s.school_dw_id
where
    st.student_username is not null and
    s.school_tenant_id = '2746328b-4109-4434-8517-940d636ffa09';
