UPDATE alefdw.dim_academic_year ac
SET academic_year_school_dw_id=sch_ac.school_dw_id
FROM (
    SELECT ac.academic_year_id, ac.academic_year_school_id, sch.school_dw_id
    FROM alefdw.dim_academic_year AS ac
        JOIN alefdw.dim_school AS sch
            ON ac.academic_year_school_id=sch.school_id
    WHERE ac.academic_year_school_dw_id IS NULL
        AND academic_year_school_id IS NOT NULL
) sch_ac
WHERE sch_ac.academic_year_school_id=ac.academic_year_school_id
    AND sch_ac.academic_year_id = ac.academic_year_id;
