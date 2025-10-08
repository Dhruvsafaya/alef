CREATE TABLE staging_tutor_translation
(
    ftt_dw_id                   bigint,
    ftt_created_time            timestamp,
    ftt_dw_created_time         timestamp,
    ftt_date_dw_id              bigint,
    ftt_user_id                 varchar(36),
    ftt_tenant_id               varchar(36),
    ftt_message_id             varchar(36),
    ftt_session_id             varchar(36),
    ftt_conversation_id        varchar(36),
    ftt_translation_language   varchar(50)
)