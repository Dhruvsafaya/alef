ALTER TABLE staging_weekly_goal ADD COLUMN fwg_date_dw_id bigint;
UPDATE staging_weekly_goal SET fwg_date_dw_id = TO_CHAR(fwg_created_time, 'YYYYMMDD')::bigint;
