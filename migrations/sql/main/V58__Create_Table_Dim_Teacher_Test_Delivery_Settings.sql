CREATE TABLE IF NOT EXISTS dim_teacher_test_delivery_settings
(
    ttds_dw_id bigint,
    ttds_test_id varchar(36),
    ttds_tenant_id varchar(36),
    ttds_test_delivery_id varchar(36),
    ttds_test_start_time timestamp,
    ttds_test_end_time timestamp,
    ttds_allow_late_submission boolean,
    ttds_title varchar(40),
    ttds_stars int,
    ttds_randomized boolean,
    ttds_delivery_status varchar(40),
    ttds_status int,
    tt_created_time timestamp,
    tt_updated_time timestamp,
    tt_deleted_time timestamp,
    tt_dw_created_time timestamp,
    tt_dw_updated_time timestamp,
    tt_dw_deleted_time timestamp
)