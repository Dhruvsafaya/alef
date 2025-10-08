CREATE TABLE IF NOT EXISTS dim_staff_user_school_role_association_v2
(
    susra_dw_id           bigint,
    susra_event_type      varchar(50),
    susra_staff_id        varchar(36),
    susra_staff_dw_id     bigint,
    susra_school_id       varchar(36),
    susra_school_dw_id    bigint,
    susra_role_name       varchar(50),
    susra_role_uuid       varchar(36),
    susra_role_dw_id      bigint,
    susra_organization    varchar(50),
    susra_status          integer,
    susra_created_time    timestamp,
    susra_dw_created_time timestamp,
    susra_active_until    timestamp
);