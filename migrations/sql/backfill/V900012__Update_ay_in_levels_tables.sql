-- Academic year tags are not updated for few records in tables fact_level_completed, fact_levels_recommended and fact_pathway_activity_completed.
-- Hence backfilling the same.
-- Ticket: https://alefeducation.atlassian.net/browse/ALEF-65424

UPDATE alefdw.fact_level_completed flc
SET flc_academic_year=extract(year from ac_year.academic_year_start_date) || '-' || extract(year from ac_year.academic_year_end_date)
    FROM
    (select flc.flc_dw_id, ac.academic_year_start_date, ac.academic_year_end_date
     from (select * from alefdw.fact_level_completed where flc_created_time>='2024-01-01 00:00:00.000000' and flc_academic_year is null)  flc
        join (select * from alefdw.dim_class where class_status=1 or class_status=4) class
             on flc.flc_class_dw_id=class.class_dw_id
        join (select * from alefdw.dim_academic_year where extract(year from academic_year_start_date) = 2024) ac
             on class.class_academic_year_id=ac.academic_year_id) as ac_year
WHERE flc.flc_dw_id=ac_year.flc_dw_id and flc.flc_academic_year is null;

UPDATE alefdw.fact_levels_recommended flr
SET flr_academic_year=extract(year from ac_year.academic_year_start_date) || '-' || extract(year from ac_year.academic_year_end_date)
    FROM
    (select flr.flr_dw_id, ac.academic_year_start_date, ac.academic_year_end_date
     from (select * from alefdw.fact_levels_recommended where flr_created_time>='2024-01-01 00:00:00.000000' and flr_academic_year is null)  flr
        join (select * from alefdw.dim_class where class_status=1 or class_status=4) class
             on flr.flr_class_dw_id=class.class_dw_id
        join (select * from alefdw.dim_academic_year where extract(year from academic_year_start_date) = 2024) ac
             on class.class_academic_year_id=ac.academic_year_id) as ac_year
WHERE flr.flr_dw_id=ac_year.flr_dw_id and flr.flr_academic_year is null;

UPDATE alefdw.fact_pathway_activity_completed fpac
SET fpac_academic_year=extract(year from ac_year.academic_year_start_date) || '-' || extract(year from ac_year.academic_year_end_date)
    FROM
    (select fpac.fpac_dw_id, ac.academic_year_start_date, ac.academic_year_end_date
     from (select * from alefdw.fact_pathway_activity_completed where fpac_created_time>='2024-01-01 00:00:00.000000' and fpac_academic_year is null)  fpac
        join (select * from alefdw.dim_class where class_status=1 or class_status=4) class
             on fpac.fpac_class_dw_id=class.class_dw_id
        join (select * from alefdw.dim_academic_year where extract(year from academic_year_start_date) = 2024) ac
             on class.class_academic_year_id=ac.academic_year_id) as ac_year
WHERE fpac.fpac_dw_id=ac_year.fpac_dw_id and fpac.fpac_academic_year is null;