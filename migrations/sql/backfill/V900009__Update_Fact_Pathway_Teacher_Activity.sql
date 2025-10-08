UPDATE alefdw.fact_pathway_teacher_activity
SET fpta_activity_type_value=CASE
                                 WHEN fact_pathway_teacher_activity.fpta_activity_type = 1 THEN 'ACTIVITY'
                                 ELSE 'INTERIM_CHECKPOINT' END
WHERE fpta_activity_type_value IS NULL
  AND fpta_action_name = 1;