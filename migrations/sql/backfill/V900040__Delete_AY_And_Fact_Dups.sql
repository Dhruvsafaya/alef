--BACKUP of records is attached in card https://alefeducation.atlassian.net/browse/ALEF-72301
delete from alefdw.dim_academic_year where academic_year_dw_id in (299724, 199220);

--Find the first occurence of fact due to duplicate ay(for validation of data), from that time delete the dups in facts due to new ay
delete from alefdw.fact_lesson_feedback where lesson_feedback_date_dw_id > 20250408 and lesson_feedback_academic_year_dw_id = 299724 and lesson_feedback_created_time >= '2025-04-09 03:09:59.691000';
delete from alefdw.fact_item_purchase where fip_date_dw_id > 20250408 and fip_academic_year_dw_id = 299724  and fip_created_time >= '2025-04-09 03:13:24.501000';
delete from alefdw.fact_learning_experience where fle_date_dw_id >= 20250409 and fle_academic_year_dw_id = 299724 and fle_created_time >= '2025-04-09 03:07:22.367000';
delete from alefdw.fact_item_transaction where fit_date_dw_id > 20250408 and fit_academic_year_dw_id = 299724 AND fit_created_time >= '2025-04-09 03:13:24.497000';