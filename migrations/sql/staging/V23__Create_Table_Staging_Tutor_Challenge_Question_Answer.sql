create table staging_tutor_challenge_question_answer
(
    ftcqa_bot_question_timestamp timestamp,
    ftcqa_bot_question_source varchar(10),
    ftcqg_bot_question_id varchar(36),
    ftcqa_bot_question_tokens integer,
    ftcqa_conversation_id varchar(36),
    ftcqa_session_id varchar(36),
    ftcqa_message_id varchar(36),
    ftcqa_user_id varchar(36),
    ftcqa_tenant_id varchar(36),
    ftcqa_date_dw_id bigint,
    ftcqa_created_time timestamp ,
    ftcqa_dw_created_time timestamp,
    ftcqa_is_answer_evaluated boolean,
    ftcqa_dw_id bigint,
    ftcqa_user_attempt_tokens int,
    ftcqa_user_attempt_number int,
    ftcqa_user_remaining_attempts int,
    ftcqa_user_attempt_timestamp timestamp,
    ftcqa_user_attempt_is_correct bool
);
