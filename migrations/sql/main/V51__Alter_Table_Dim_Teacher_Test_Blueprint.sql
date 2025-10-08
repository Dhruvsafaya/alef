DROP TABLE dim_teacher_test_blueprint;
CREATE TABLE dim_teacher_test_blueprint
(
    ttb_dw_id                             bigint,
    ttb_created_time                      timestamp,
    ttb_updated_time                      timestamp,
    ttb_deleted_time                      timestamp,
    ttb_dw_created_time                   timestamp,
    ttb_dw_updated_time                   timestamp,
    ttb_dw_deleted_time                   timestamp,
    ttb_test_blueprint_id                 varchar(36),
    ttb_tenant_id                         varchar(36),
    ttb_test_blueprint_class_id           varchar(36),
    ttb_test_blueprint_title              varchar(50),
    ttb_test_blueprint_domain_id          varchar(36),
    ttb_test_blueprint_guidance_type      varchar(50),
    ttb_test_blueprint_major_version      int,
    ttb_test_blueprint_minor_version      int,
    ttb_test_blueprint_revision_version   int,
    ttb_test_blueprint_number_of_question int,
    ttb_test_blueprint_created_by_id      varchar(36),
    ttb_test_blueprint_updated_by_id      varchar(36),
    ttb_test_blueprint_published_by_id    varchar(36),
    ttb_test_blueprint_status             varchar(40),
    ttb_status                            int
)