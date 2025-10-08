CREATE TABLE IF NOT EXISTS rel_user_v2
(
    user_dw_id           bigint identity (1,1),
    user_id              varchar(36),
    user_type            varchar(50),
    user_created_time    timestamp,
    user_dw_created_time timestamp
)
    SORTKEY
        (
        user_dw_id
        );