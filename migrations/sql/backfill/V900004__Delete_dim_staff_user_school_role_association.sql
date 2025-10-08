DELETE FROM alefdw.dim_staff_user_school_role_association
WHERE susra_staff_id IN
      (SELECT susra_staff_id
       FROM alefdw.dim_staff_user_school_role_association
       WHERE susra_status = 1
       GROUP BY susra_staff_id, susra_school_id, susra_role_uuid
       HAVING count(1) > 1)
  AND susra_status = 1 AND (susra_event_type='UserDisabledEvent' OR susra_event_type='UserEnabledEvent');