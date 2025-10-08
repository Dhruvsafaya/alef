-- https://alefeducation.atlassian.net/browse/ALEF-69402
-- backups.dim_teacher_test_blueprint_lesson_association_2025_01_14
DELETE
FROM alefdw.dim_teacher_test_blueprint_lesson_association
WHERE ttbla_dw_id IN
      (SELECT a.ttbla_dw_id
       FROM (SELECT *,
                    dense_rank() over (partition BY ttbla_test_blueprint_id, ttbla_lesson_id ORDER BY ttbla_dw_id) AS rnk
             FROM alefdw.dim_teacher_test_blueprint_lesson_association) a
       WHERE a.rnk > 1);

-- backups.dim_teacher_test_item_association_2025_01_14

DELETE
FROM alefdw.dim_teacher_test_item_association
WHERE ttia_dw_id in
      (SELECT a.ttia_dw_id
       FROM (SELECT *, dense_rank() over (partition BY ttia_test_id, ttia_test_item_id ORDER BY ttia_dw_id) AS rnk
             FROM alefdw.dim_teacher_test_item_association) a
       WHERE a.rnk > 1);