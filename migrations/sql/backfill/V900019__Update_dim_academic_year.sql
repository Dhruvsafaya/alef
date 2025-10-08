----https://alefeducation.atlassian.net/browse/ALEF-68565
-- continuation of V900018 and V900017 migrations , a side effect will be present on dim_academic_year_table which is handled to mark the completion of the previous academic year
update alefdw.dim_academic_year set academic_year_is_roll_over_completed=true where academic_year_id='757332fc-77fb-43b4-b0ad-bb3a63b9dc50';
update alefdw.dim_academic_year set academic_year_state='CONCLUDED' where academic_year_id='757332fc-77fb-43b4-b0ad-bb3a63b9dc50';
update alefdw.dim_academic_year set academic_year_updated_time='2024-11-04 05:47:48.000000' where academic_year_id='757332fc-77fb-43b4-b0ad-bb3a63b9dc50';
update alefdw.dim_academic_year set academic_year_dw_updated_time='2024-11-04 05:47:48.000000' where academic_year_id='757332fc-77fb-43b4-b0ad-bb3a63b9dc50';