CREATE TABLE fact_tutor_analogous
(
    fta_dw_id           bigint,
    fta_created_time    timestamp,
    fta_dw_created_time timestamp,
    fta_date_dw_id      bigint,
    fta_user_id         varchar(36),
    fta_user_dw_id      bigint,
    fta_tenant_id       varchar(36),
    fta_message_id      varchar(36),
    fta_session_id      varchar(36),
    fta_conversation_id varchar(36)
)