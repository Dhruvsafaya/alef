INSERT INTO alefdw_stage.test_table_stage (id) VALUES (228);

INSERT INTO alefdw.test_table
SELECT *
FROM alefdw_stage.test_table_stage;
