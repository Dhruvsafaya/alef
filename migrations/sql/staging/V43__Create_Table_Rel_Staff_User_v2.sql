CREATE TABLE IF NOT EXISTS rel_staff_user_v2
(
    rel_staff_user_dw_id           bigint,
    staff_user_event_type          varchar(50),
    staff_user_created_time        timestamp,
    staff_user_dw_created_time     timestamp,
    staff_user_active_until        timestamp,
    staff_user_status              integer,
    staff_user_id                  varchar(36),
    staff_user_onboarded           boolean,
    staff_user_expirable           boolean DEFAULT false,
    staff_user_exclude_from_report boolean DEFAULT false,
    staff_user_avatar              varchar(100),
    staff_user_enabled             boolean
);