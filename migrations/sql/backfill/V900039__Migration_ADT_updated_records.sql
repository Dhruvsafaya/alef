--BACKUP of records WHERE ADT ATTEMPT 2 IS COMPLETED AND ADT ATTEMPT 3 IS AGAIN TAKEN BY STUDENTS IN TERM 2
----backups.records_to_be_discarded_ALEF_72018

--BACKUP of records WHERE only ADT ATTEMPT 3 IS TAKEN BY STUDENTS IN TERM 2
----backups.records_to_be_updated_ALEF_72018

--The new update adt timerange for specified schools is saved in backups.adtthreshold_new_timerange_ALEF_72018 table

--MARK LEARNING SESSION AS DISCARDED WHERE ADT ATTEMPT 2 IS COMPLETED AND ADT ATTEMPT 3 IS AGAIN TAKEN BY STUDENTS IN TERM 2
update alefdw.fact_learning_experience set fle_state = 4
from alefdw.fact_learning_experience fle
join backups.records_to_be_discarded_ALEF_72018 disacarded on fle.fle_ls_id = disacarded.fasr_fle_ls_uuid
where fle.fle_date_dw_id >= 20241206 and fle.fle_attempt = 3 and fle.fle_state = 2;

update alefdw.fact_adt_student_report set fasr_status = 4
from alefdw.fact_adt_student_report fasr
join backups.records_to_be_discarded_ALEF_72018 disacarded on fasr.fasr_fle_ls_uuid = disacarded.fasr_fle_ls_uuid
where fasr.fasr_date_dw_id >= 20241206 and fasr.fasr_attempt = 3 and fasr.fasr_status = 1;

update alefdw.fact_adt_next_question set fanq_status = 4
from alefdw.fact_adt_next_question fanq
join backups.records_to_be_discarded_ALEF_72018 disacarded on fanq.fanq_fle_ls_uuid = disacarded.fasr_fle_ls_uuid
where fanq.fanq_date_dw_id >= 20241206 and fanq.fanq_attempt = 3 and fanq.fanq_status = 1;


--UPDATE LEARNING SESSION, FACT_ADT_STUDENT_REPORT AND FACT_ADT_NEXT_QUESTION TO ATTEMPT = 2 WHERE ADT ATTEMPT 3 IS TAKEN BY STUDENTS IN TERM 2
update alefdw.fact_learning_experience set fle_attempt = 2
from alefdw.fact_learning_experience fle
join backups.records_to_be_updated_ALEF_72018 update_with_attempt_2 on fle.fle_ls_id = update_with_attempt_2.fasr_fle_ls_uuid
where fle.fle_date_dw_id >= 20241206 and fle.fle_attempt = 3;

update alefdw.fact_adt_student_report set fasr_attempt = 2
from alefdw.fact_adt_student_report fasr
join backups.records_to_be_updated_ALEF_72018 update_with_attempt_2 on fasr.fasr_fle_ls_uuid = update_with_attempt_2.fasr_fle_ls_uuid
where fasr.fasr_date_dw_id >= 20241206 and fasr.fasr_attempt = 3;

update alefdw.fact_adt_next_question set fanq_attempt = 2
from alefdw.fact_adt_next_question fanq
join backups.records_to_be_updated_ALEF_72018 update_with_attempt_2 on fanq.fanq_fle_ls_uuid = update_with_attempt_2.fasr_fle_ls_uuid
where fanq.fanq_date_dw_id >= 20241206 and fanq.fanq_attempt = 3;

------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Use the data from backups.adtthreshold_new_timerange_ALEF_72018 table, and mark the old active records as `INACTIVE`
update alefdw.dim_adt_attempt_threshold set aat_status = 2,
aat_state = 'INACTIVE',
aat_updated_time = cast(sa.updated_on as timestamp),
aat_dw_updated_time = '2025-04-03 08:55:00.000000'
from alefdw.dim_adt_attempt_threshold th
join alefdw.dim_school s on th.aat_school_dw_id = s.school_dw_id
join backups.adtthreshold_new_timerange_ALEF_72018 sa
on
th.aat_school_id = sa.school_id and
th.aat_attempt_number = sa.attempt_number and
th.aat_attempt_start_time != cast(sa.attempt_start_date as timestamp) and
th.aat_attempt_end_time != cast(sa.attempt_end_date as timestamp)
where
th.aat_state = 'ACTIVE' and
s.school_id in (
'ca85914d-1e4c-44d4-a648-22b134af6668',
'1caf63e5-461e-45ea-85b2-358b72f98e4c',
'fc4f084d-2d46-4a90-9ece-dad859d1aecf',
'2c6efbe9-610a-4c09-8106-0d83eec52d42',
'23eb2c18-b99a-48e2-9aa2-c6ce5de60fde',
'4c6ee44b-9217-4b4b-80da-35d791ba33c6',
'1cad621b-a37f-40fd-bd81-3df0ba2d06d4',
'3512662e-a9a4-4a68-bbd0-8a9e60656de9'
);

-- Use the data from backups.adtthreshold_new_timerange_ALEF_72018 table and insert fresh records with `ACTIVE` status
insert into alefdw.dim_adt_attempt_threshold
(aat_created_time,
aat_updated_time,
aat_dw_created_time,
aat_dw_updated_time,
aat_status,
aat_id,
aat_tenant_dw_id,
aat_tenant_id,
aat_academic_year_dw_id,
aat_academic_year_id,
aat_school_dw_id,
aat_school_id,
aat_state,
aat_attempt_title,
aat_attempt_start_time,
aat_attempt_end_time,
aat_attempt_number,
aat_total_attempts)
  select
  cast(sa.updated_on as timestamp) as created_time,
  null,
  '2025-04-03 08:55:00.000000' as dw_created_time,
  null,
  1,
  aat_id,
  aat_tenant_dw_id,
  aat_tenant_id,
  aat_academic_year_dw_id,
  aat_academic_year_id,
  aat_school_dw_id,
  aat_school_id,
  sa.status,
  sa.attempt_title,
  cast(sa.attempt_start_date as timestamp) as attempt_start_date,
  cast(sa.attempt_end_date as timestamp) as attempt_end_date,
  sa.attempt_number,
  aat_total_attempts
  from alefdw.dim_adt_attempt_threshold th
  join alefdw.dim_school s on th.aat_school_dw_id = s.school_dw_id
  join backups.adtthreshold_new_timerange_ALEF_72018 sa
  on
  th.aat_state = 'ACTIVE' and
  th.aat_school_id = sa.school_id and
  th.aat_attempt_number = sa.attempt_number and
  th.aat_attempt_start_time != cast(sa.attempt_start_date as timestamp) and
  th.aat_attempt_end_time != cast(sa.attempt_end_date as timestamp)
  where s.school_id in (
  'ca85914d-1e4c-44d4-a648-22b134af6668',
  '1caf63e5-461e-45ea-85b2-358b72f98e4c',
  'fc4f084d-2d46-4a90-9ece-dad859d1aecf',
  '2c6efbe9-610a-4c09-8106-0d83eec52d42',
  '23eb2c18-b99a-48e2-9aa2-c6ce5de60fde',
  '4c6ee44b-9217-4b4b-80da-35d791ba33c6',
  '1cad621b-a37f-40fd-bd81-3df0ba2d06d4',
  '3512662e-a9a4-4a68-bbd0-8a9e60656de9'
  );
