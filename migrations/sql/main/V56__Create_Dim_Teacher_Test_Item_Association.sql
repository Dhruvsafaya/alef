CREATE TABLE IF NOT EXISTS dim_teacher_test_item_association
(
    ttia_dw_id bigint,
    ttia_test_id varchar(36),
    ttia_item_id varchar(36),
    ttia_status int,
    ttia_dw_created_time timestamp,
    ttia_created_time timestamp,
    ttia_active_until timestamp
)