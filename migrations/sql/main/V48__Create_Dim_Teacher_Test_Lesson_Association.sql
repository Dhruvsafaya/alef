CREATE TABLE dim_teacher_test_blueprint_lesson_association
(
    ttbla_dw_id bigint,
    ttbla_test_blueprint_id varchar(36),
    ttbla_lesson_id varchar(36),
    ttbla_status int,
    ttbla_dw_created_time timestamp,
    ttbla_created_time timestamp
)