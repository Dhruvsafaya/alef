--ALEF-71375 DELETE dups of fact learning experience
delete
from alefdw.fact_learning_experience
where fle_date_dw_id = 20250227 and fle_dw_created_time = '2025-02-27 10:29:05.000000'
and fle_exp_id in
(
'7c45aa10-9447-4108-b790-2ffb96596ff5',
'98610e41-4f34-4968-a653-3f016f3caae0',
'd2e35346-35d5-4f34-a690-4b1de0837cff',
'e904cde9-5733-4f7e-a2bc-cc206554921f',
'fe753c27-e67b-47b3-aae5-d1991afd63e9',
'e04b2bc6-685c-4720-92e5-73f51a82b266',
'd36b5333-def3-4824-bec0-906a0f1ce292',
'c249a825-7cfd-4cb7-86f3-60ab4c66736f'
);
