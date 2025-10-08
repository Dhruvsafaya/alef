-- Bug caught in Data Comparison as part of falcon quality alerts https://alefeducation.atlassian.net/browse/ALEF-66844 https://alefeducation.atlassian.net/browse/ALEF-67367
-- Note : Using timestamp to update the correct school in  historical records of the students
--Update script for Pickett Elementary School

update alefdw.dim_student set student_school_dw_id=137748 where student_id IN ('5fc1a794-6165-4414-87fc-ff83de0dc6a6') and student_created_time>='2024-10-02 00:00:00.000000';
