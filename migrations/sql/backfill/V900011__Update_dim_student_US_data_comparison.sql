-- Bug caught in Data Comparison as part of falcon quality alerts https://alefeducation.atlassian.net/browse/ALEF-66844 https://alefeducation.atlassian.net/browse/ALEF-67367
-- Note : Using timestamp to update the correct school in  historical records of the students
-- Update script for Southwest Elementary Global Academy
update alefdw.dim_student set student_school_dw_id=139527 where student_id IN ('72b312dc-5097-40d0-a8e3-00e8f5d94886',
                                                                               'b8b8863a-d5e0-4fa9-a8cc-10aa59bc681f',
                                                                               '1ba91a17-0da1-4b2f-94af-4777b33a8f34',
                                                                               '2e0561d4-bcd0-4872-ab8c-d23ca152c6ea',
                                                                               '518ff23e-e8ca-4123-80d0-3379a6b9f5aa',
                                                                               'e0942692-ea13-4ae0-a771-ebc3a857818c',
                                                                               '4a5e62b2-4c0c-4d26-8283-150d9269fb63',
                                                                               'c3c6b6e5-0aa9-4027-a94d-c3ebafa561a1',
                                                                               'd0309263-f845-4d53-b90b-1813fd08bc5d',
                                                                               '51059269-84c4-4259-b47b-6c8ae5b694cd',
                                                                               '0fe4d911-47af-4c28-8a62-7a112ee11f78') and student_created_time>='2024-10-02 00:00:00.000000';