DELETE FROM alefdw.dim_interim_checkpoint WHERE ic_id = '0f9bd47e-ed05-46b9-aa17-4d87232e73ec' AND ic_dw_id = 25931;


DELETE FROM alefdw.dim_pacing_guide
WHERE pacing_activity_id = '0f9bd47e-ed05-46b9-aa17-4d87232e73ec'
    AND pacing_activity_dw_id = 25931 and pacing_status = 1;


DELETE FROM alefdw.dim_course_activity_association
WHERE caa_course_id = 'ae74ba56-248e-4d8b-b88a-3a6f0c751787' AND
    caa_activity_id = '0f9bd47e-ed05-46b9-aa17-4d87232e73ec'
  AND caa_status = 1 AND caa_activity_dw_id = 25931;
