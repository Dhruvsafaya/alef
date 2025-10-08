-- Bug caught in Data Comparison as part of falcon quality alerts https://alefeducation.atlassian.net/browse/ALEF-66844 https://alefeducation.atlassian.net/browse/ALEF-67367
-- Note : Using timestamp to update the correct school in  historical records of the students
--Update script for Pickett Elementary School

update alefdw.dim_student set student_school_dw_id=137754 where student_id IN ('eee2d7db-2d21-4c73-8c99-3b57bee03253', '584de769-e8e7-4aa3-881d-ab76853c0e47', 'a92a358a-ba28-48bb-a0af-54a429d039e0', 'c374ee77-18dc-432c-84ee-06c29e146377', '1bf3f86a-0a38-4c15-b949-1a0990c95ab0', '53f51494-a791-4cbb-b118-2b67d680be7d', '38f8df42-360b-4342-9314-819b304c0e94') and student_created_time>='2024-10-02 00:00:00.000000';
