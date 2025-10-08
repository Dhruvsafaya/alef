ALTER TABLE fact_adaptive_practice_progress ADD COLUMN skill_proficiency_score_tmp DOUBLE PRECISION;
ALTER TABLE fact_adaptive_practice_progress ADD COLUMN level_proficiency_score_tmp DOUBLE PRECISION;
ALTER TABLE fact_adaptive_practice_progress ADD COLUMN answer_score_tmp DOUBLE PRECISION;

UPDATE fact_adaptive_practice_progress
SET
    skill_proficiency_score_tmp = skill_proficiency_score,
    level_proficiency_score_tmp = level_proficiency_score,
    answer_score_tmp = answer_score;

ALTER TABLE fact_adaptive_practice_progress DROP COLUMN skill_proficiency_score;
ALTER TABLE fact_adaptive_practice_progress DROP COLUMN level_proficiency_score;
ALTER TABLE fact_adaptive_practice_progress DROP COLUMN answer_score;

ALTER TABLE fact_adaptive_practice_progress RENAME COLUMN skill_proficiency_score_tmp TO skill_proficiency_score;
ALTER TABLE fact_adaptive_practice_progress RENAME COLUMN level_proficiency_score_tmp TO level_proficiency_score;
ALTER TABLE fact_adaptive_practice_progress RENAME COLUMN answer_score_tmp TO answer_score;
