alter table fact_inc_game add column inc_game_is_assessment BOOLEAN default false;
alter table fact_inc_game_outcome add column inc_game_outcome_is_assessment BOOLEAN default false;
alter table fact_inc_game_session add column inc_game_session_is_assessment BOOLEAN default false;
