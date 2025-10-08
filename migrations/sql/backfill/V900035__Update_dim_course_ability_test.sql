UPDATE alefdw.dim_course_ability_test_association SET cata_ability_test_activity_type = 'TestActivity' WHERE cata_ability_test_activity_type IS NULL;
UPDATE alefdw.dim_course_ability_test_association SET cata_is_placement_test = true WHERE cata_is_placement_test IS NULL;
