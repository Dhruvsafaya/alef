INSERT INTO alefdw_stage.rel_user (user_id, user_type, user_created_time, user_dw_created_time)
SELECT '99bad7d5-ed49-4be6-9b42-35b31ee92597', 'GUARDIAN', '2025-01-01 12:03:58.403000', current_timestamp
    WHERE NOT EXISTS (SELECT 1 FROM alefdw_stage.rel_user WHERE user_id = '99bad7d5-ed49-4be6-9b42-35b31ee92597');


INSERT INTO alefdw_stage.rel_guardian (rel_guardian_dw_id, guardian_created_time, guardian_dw_created_time, guardian_active_until, guardian_status, guardian_id, student_id, guardian_invitation_status)
SELECT u.user_dw_id as rel_guardian_dw_id,
       u.user_created_time as guardian_created_time,
       current_timestamp as guardian_dw_created_time,
       null as guardian_active_until,
       1 as guardian_status,
       '99bad7d5-ed49-4be6-9b42-35b31ee92597' as guardian_id,
       'cd27ca6f-f155-43e7-a5f2-079f3aaf0ecf' as stusent_id,
       2 as guardian_invitation_status
FROM alefdw_stage.rel_user u WHERE u.user_id = '99bad7d5-ed49-4be6-9b42-35b31ee92597'
AND NOT EXISTS (SELECT 1 FROM alefdw_stage.rel_guardian WHERE guardian_id = '99bad7d5-ed49-4be6-9b42-35b31ee92597'
                UNION ALL
                SELECT 1 FROM alefdw.dim_guardian WHERE guardian_id = '99bad7d5-ed49-4be6-9b42-35b31ee92597');
