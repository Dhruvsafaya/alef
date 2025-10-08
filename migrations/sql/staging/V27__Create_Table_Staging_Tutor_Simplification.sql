CREATE TABLE staging_tutor_simplification
(
    fts_dw_id                   bigint,
    fts_created_time            timestamp,
    fts_dw_created_time         timestamp,
    fts_date_dw_id              bigint,
    fts_user_id                 varchar(36),
    fts_tenant_id               varchar(36),
    fts_message_id             varchar(36),
    fts_session_id             varchar(36),
    fts_conversation_id        varchar(36)
)