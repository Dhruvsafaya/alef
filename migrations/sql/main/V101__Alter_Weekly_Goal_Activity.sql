ALTER TABLE fact_weekly_goal_activity ADD COLUMN fwga_date_dw_id bigint;
UPDATE fact_weekly_goal_activity SET fwga_date_dw_id = TO_CHAR(fwga_created_time, 'YYYYMMDD')::bigint;

ALTER TABLE fact_weekly_goal ADD COLUMN fwg_date_dw_id bigint;
UPDATE fact_weekly_goal SET fwg_date_dw_id = TO_CHAR(fwg_created_time, 'YYYYMMDD')::bigint;