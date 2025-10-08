CREATE TABLE fact_tutor_onboarding
(
    fto_dw_id                   bigint,
    fto_created_time            timestamp,
    fto_dw_created_time         timestamp,
    fto_date_dw_id              bigint,
    fto_user_id                 varchar(36),
    fto_user_dw_id              bigint,
    fto_tenant_id               varchar(36),
    fto_question_id             varchar(36),
    fto_question_category       varchar(50),
    fto_user_free_text_response bool,
    fto_onboarding_complete     bool,
    fto_onboarding_skipped      bool
)