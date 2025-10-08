DELETE FROM alefdw.dim_staff_user
WHERE staff_user_id IN
      (SELECT staff_user_id FROM alefdw.dim_staff_user
       WHERE staff_user_status = 1
       GROUP BY staff_user_id
       HAVING count(1) > 1)
  AND staff_user_status = 1 AND (staff_user_event_type='UserDisabledEvent' OR staff_user_event_type='UserEnabledEvent');
