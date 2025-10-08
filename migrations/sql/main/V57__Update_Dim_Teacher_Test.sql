ALTER TABLE dim_teacher_test ADD COLUMN tt_test_blueprint_id varchar(36);
ALTER TABLE dim_teacher_test_item_association RENAME COLUMN ttia_item_id to ttia_test_item_id;
