--https://alefeducation.atlassian.net/browse/ALEF-68565
---
-- continuation of V900018 and V900017 migrations , timestamp was wrong and fixing the same to represent the exact time from app db


update alefdw.dim_school_academic_year_association set saya_created_time='2024-11-04 12:50:40.902000' where saya_dw_id=161324;

update alefdw.dim_school_academic_year_association set saya_dw_created_time='2024-11-04 12:50:40.902000' where saya_dw_id=161324;