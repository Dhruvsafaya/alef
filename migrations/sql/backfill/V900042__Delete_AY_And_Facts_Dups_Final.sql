-- backups are attached in ALEF-64473 jira
-- deleting all fact dups first
delete from alefdw.fact_learning_experience where fle_academic_year_dw_id=299736;
delete from alefdw.fact_lesson_feedback where lesson_feedback_academic_year_dw_id=299736;
delete from alefdw.fact_experience_submitted where fes_academic_year_dw_id=299736;
delete from alefdw.fact_student_certificate_awarded where fsca_academic_year_dw_id=299736;
delete from alefdw.fact_badge_awarded where fba_academic_year_dw_id=299736;
delete from alefdw.fact_item_purchase where fip_academic_year_dw_id=299736;
delete from alefdw.fact_item_transaction where fit_academic_year_dw_id=299736;
delete from alefdw.fact_star_awarded where fsa_academic_year_dw_id=299736;
delete from alefdw.fact_ktg_session where ktg_session_academic_year_dw_id=299736;
delete from alefdw.fact_ktg where  ktg_academic_year_dw_id=299736;

-- deleting base dups of ay

delete from alefdw.dim_academic_year where academic_year_dw_id=299736;

-- deleting base dups of ay and please note that below ay has no facts associated and hence deleting the below as well

delete from alefdw.dim_academic_year where academic_year_dw_id IN (299742,392602,299748,373165,392608);