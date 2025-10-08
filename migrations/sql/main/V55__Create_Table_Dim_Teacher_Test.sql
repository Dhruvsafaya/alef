CREATE TABLE IF NOT EXISTS dim_teacher_test
(
    tt_dw_id bigint,
    tt_test_id varchar(36),
    tt_tenant_id varchar(36),
    tt_test_class_id varchar(36),
    tt_test_title varchar(50),
    tt_test_domain_id varchar(36),
    tt_test_status varchar(50),
    tt_test_created_by_id varchar(36),
    tt_status int,
    tt_test_updated_by_id varchar(36),
    tt_test_published_by_id varchar(36),
    tt_created_time timestamp,
    tt_updated_time timestamp,
    tt_deleted_time timestamp,
    tt_dw_created_time timestamp,
    tt_dw_updated_time timestamp,
    tt_dw_deleted_time timestamp
)