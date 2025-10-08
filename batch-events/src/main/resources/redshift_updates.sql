-- updating mlo title manually as it is manually updated in source (Story: ALEF-8538)
update alefdw.dim_learning_objective
set lo_title='شعر - قيمة العلم - الجزء الأوّل'
where lo_id='98ee6be9-d0ea-47b9-89e2-4677e41f6dc3';

update alefdw.dim_learning_objective
set lo_title='شعر - قيمة العلم - الجزء الثّاني'
where lo_id='3a04558e-6edd-4660-b73c-5995c2450703';

-- Update school_tenant_id column for US schools (Story: ALEF-9441)
update devalefdw.dim_school
set school_tenant_id='2746328b-4109-4434-8517-940d636ffa09'
where school_id in ('62e57b85-e854-4719-967a-307ee9ba5822','72b2c783-81d0-432a-8024-b612e4a4d0d4');

-- Update school_tenant_id column for Private schools (Story: ALEF-9441)
update devalefdw.dim_school
set school_tenant_id='21ce4d98-8286-4f7e-b122-03b98a5f3b2e'
where school_id in ('1a421f4c-a018-4f64-aa5c-1b49bfb296fe',
'1c1d2e42-2a9d-4f6d-807c-d8571d9fd461',
'1f135b2d-545d-4d4c-90b5-25b84cab0db3',
'29538d47-fdfd-430b-a12d-5cfdb8586adc',
'30f09560-146d-4530-af3a-b1237809d8d3',
'5ba75a03-3095-4a32-9615-9aee26842e12',
'76e83607-3a13-43a5-b027-671bc40b34a7',
'7a39f894-97e4-4a85-9726-1f895dd67ad0',
'8f677257-7084-460e-8124-9880378e9465',
'9f17c4fc-7b02-4593-a0f8-1450485c88d6',
'a681de2c-49a3-4175-970f-a6016e1d89ba',
'b278ed55-e5a9-4787-aca8-78b8df38fae5',
'c9815a76-4d65-4510-bc81-fa48ff7368b1',
'd0681d44-793c-478a-b6ef-751bb0701460',
'de428c28-abf6-4efe-babe-be7adb658ebf',
'deffa42d-b04a-4c32-8e07-a432c2b9c1cf');

-- Update school_tenant_id column for MOE schools (Story: ALEF-9441)
update devalefdw.dim_school
set school_tenant_id='93e4949d-7eff-4707-9201-dac917a5e013'
where school_id in ('0433fff1-fb76-493b-add3-313cc53f1499',
'05fbea9a-3d29-495e-9c2d-1f9bff8373bd',
'0627138d-2c63-4ec5-a4a8-4b48c57399f2',
'07a1816e-77aa-43bc-a6bb-79df50d3fb50',
'0fc3afff-ec79-480b-9318-68194ca4b5f7',
'0fdbc16e-ca83-4181-b21f-3836cde1c5e8',
'10309e9c-93b9-47a0-b47d-915102fe1859',
'104e20f9-8605-4fa6-9309-860f7452c220',
'1537d4a9-210a-400e-86f7-675ce145d57c',
'15ce5b25-5511-4996-a22f-9dde5c7c40cc',
'1658cfad-102c-4e45-8a05-3726c334342e',
'1a6bd49a-a383-4268-851a-6be46ce3d6ec',
'1aebf2a9-dc77-46da-b64d-72ee10eeac24',
'1caf63e5-461e-45ea-85b2-358b72f98e4c',
'20884b4a-417f-44f7-b3d0-0d2cca7974a2',
'23eb2c18-b99a-48e2-9aa2-c6ce5de60fde',
'251d413c-3532-4e00-8144-137020ec40b8',
'29d5a080-9d97-4557-9050-a8489270c975',
'2bcd5cf3-f840-485f-be8d-0a683344781e',
'2c38e83f-b24c-43d7-8454-c68f36e7d661',
'2c6efbe9-610a-4c09-8106-0d83eec52d42',
'2e5d9bd8-14ff-4da0-ae3f-ca67394f9fbf',
'2f4c9dab-8d1e-4444-9128-e6e8b12a73e5',
'307c0bc7-2452-4eac-9848-f118b5b6f140',
'320a3723-802f-4cf5-9efe-e1bc348a7839',
'344cc136-65fe-45fc-beec-3753fe8e49eb',
'3481873e-21c6-4922-b458-1298a5e3ce90',
'35047d8e-76dd-4b89-bb28-410299e79309',
'35ed335e-17ea-4451-9a9f-22e0ec3bb900',
'38a2a746-aa15-4bd6-a9f0-58cda0f5a6dc',
'3918b584-eea1-4515-a6eb-956b8df0077a',
'3922cf1c-33c8-4879-a6fa-83e1c5ba1e85',
'39cd6b41-ce90-4a01-a043-54707ad5ca41',
'3a6fe0c7-b0bd-4a80-b783-e6bd8d7db38a',
'3cba92ad-588f-4b85-bdef-8c22c1e6d6f6',
'3de9dbc0-6f62-48a0-9468-abba3adeb0e7',
'44dbf418-a203-4506-a452-b0766d2c7c7a',
'4b002c87-cb02-474c-973e-916a9c873319',
'4c5b68fb-2bbb-42e2-a26b-4857a19eb70b',
'4d271e3c-a45a-4d74-89c2-ab677590fd0f',
'4d8159d4-08ad-4427-b863-f4693904d76a',
'4e89f61c-142d-4fd4-98f8-86d048065761',
'4ed2618c-2fe7-4e4a-893d-6a02d691a9e2',
'4f0fbaae-5105-44be-8ecc-92993af27b82',
'4f9fcb50-f66e-4474-a99a-82ee11e383e0',
'4fb01b51-65e8-4365-a7c0-bc3fdfcd6365',
'52eddd88-0500-4b0f-8bbc-82a14430f6de',
'55969308-d2b6-4164-917b-c00300f7d066',
'57f1f988-608e-4db3-a561-816369cabf67',
'5a6f7a9b-19eb-485e-a58e-eadda4a5f28c',
'60e770eb-2294-4389-b24e-4c1ea11b3206',
'61c5862e-3f07-41e0-ad54-ef646c9d7be1',
'61ea198b-af11-4702-9fa9-5a79cc125cb4',
'66a900b2-d1a5-4c73-96b4-6edddcce64a9',
'674325c3-e80e-400f-9d23-7936da6ae806',
'679126f2-c16b-4c0c-b21a-08f8d6caded7',
'6a161092-137c-4040-9413-e755836c916a',
'6a1b5604-2a3a-45d9-8d1f-ed5adf6c7b12',
'6c881224-12f8-422c-82cf-ab1fb83d4371',
'6dafa1e4-6183-4cf4-bbff-3bc21b92feb7',
'73835f25-bdd2-4d71-83f3-99569f09be5e',
'75d9193d-b0c0-4aa8-bd7e-88c7aa90d99a',
'777e3cbb-6c33-4666-acfa-524f35d296e1',
'7ec77a4c-b535-40b7-afbd-e92f9f7ce7a9',
'80003d96-b448-4d12-8625-ac0ce027221b',
'84a2930b-b532-4a38-bce7-c72437ded148',
'86727589-230a-4d2c-87c6-8f714aa4a68d',
'868d4fa4-c13d-47a5-961d-faab69fcea8f',
'86c7acbc-bf15-4c5a-8c06-294376cbfd4f',
'8aa1ffa4-e754-4364-b0d6-5daa40d2c609',
'8ac082a4-6ec5-4ed4-a871-bcf7dd7f8384',
'8c095baf-940d-4c54-b702-386826f3366e',
'8c7edfc4-a37e-41d8-a78d-e44bbafe8629',
'8d725aa6-4c7f-4231-9273-f0c4796c362a',
'8e4e65b9-ba15-4590-9e46-cac0c77b7b4d',
'8e58ffd1-fe36-40e1-af06-cee908c5ea05',
'90a5d81f-d659-4b7b-bbbb-f64989d9d84a',
'93c7e412-65cb-4964-96dd-cd623c160672',
'99005bac-15a9-4f17-b238-f74fe3dbde52',
'9bb50ddc-43a3-47ca-8b01-c4ca2006257f',
'9d692fea-8426-41b1-9b64-cdc27b89d559',
'9d833e0d-f3e0-4696-a574-80eb34469450',
'9d9ae7f5-2aa1-438a-8bb3-87af67230fe7',
'9f00ee5a-c36d-41d9-82d5-1de41408e730',
'a067d4f7-31c2-4042-b619-bd00fa24fe36',
'a0e99be7-7772-4d90-9015-e9d81a956bd7',
'a18a5c56-c076-46de-b71c-016acb6f1b5e',
'a2569d23-11d3-4384-8d33-26b795ed33bf',
'a42c46cc-0b68-42e4-b529-4fdb99404ed7',
'a6505bbe-1e63-4b7a-ae4e-3e769e05ec1e',
'a7e1424b-577c-4439-b70f-0620ea4d1557',
'a8cafcb1-ff73-4109-8335-02118b0f7476',
'a8cc2053-4440-49d3-bb01-9a4b21dd311c',
'afa3caae-f37e-4f27-97d4-0716e4bb41f7',
'b1a3d9c7-39ad-4772-8927-5e0c07db16ca',
'b2a645bd-dbed-41d2-9798-21370973accd',
'b371f4c5-16b4-4cd2-9a33-9c9c2dd87f5c',
'b377ed0b-9190-4db9-a8b2-83a02f7fd856',
'b4cfcf31-21b8-45dd-9583-4e765da851ac',
'b69c4565-ec1f-485d-86eb-1d4891bec29f',
'b7fa68a7-1c0e-426b-bf55-2cc309915e86',
'be798b4e-8b1e-4c57-b7db-e1889645c051',
'be844718-d26f-4385-a85e-2226a2539518',
'bf200432-b598-42c9-be8d-3c4b175ba7fb',
'c288cfee-83b6-4d6f-9d5a-ee73d905b80e',
'c29f7e2f-5216-4229-8ff8-59545f78c5d3',
'c60a3dd1-498e-4cbf-a47f-f6f76ebc0388',
'c97921a8-6555-4c5f-acd2-9487803f2b66',
'ca31eafd-3e34-4ddd-98eb-c4f5981de3d3',
'ca85914d-1e4c-44d4-a648-22b134af6668',
'cb1c3052-8ce9-4437-8cd1-e942172dfb78',
'cc1acc48-a74a-40bc-9527-cdd560968e2b',
'cf38e73f-0750-4f63-9a1f-4a651d61e238',
'cfb30d4b-934d-4cba-afa5-5642eafd3e1e',
'd16cf0f0-0e31-4518-8798-651a86bc6505',
'd182836d-f510-4980-bfed-813989312074',
'd6343d0e-4db2-4edf-869e-d68cdf685588',
'd7bdc175-9bdd-4a70-bad7-219866039ab1',
'd871e23e-b55a-40d2-b7a8-20928481199a',
'dabef08d-ba53-4693-a586-a7b921001939',
'dbab7d20-4831-46b2-acd5-1fa1622a28e5',
'dcad75fd-2dbc-4eef-bdef-e1996383cceb',
'e2b8f588-877d-4b34-b2cc-dabf67b2a2f6',
'e3415d77-26aa-4370-bdb5-54a7b0842e73',
'e3cff422-904b-4a66-b5c9-408c62443a60',
'e431865d-f8de-4f4c-942a-c69710d6ff40',
'e817ee37-b171-4086-bcf8-a9aaf5daedef',
'e8305c82-3235-439e-b7c4-fb53c4392be6',
'e95ff88b-3bcf-4ccc-a867-ec783a79f3aa',
'f201099c-6f5b-40fc-9544-40ea9dc0c4de',
'f2cd9c55-90cc-43b0-9481-f3fe166475b0',
'f3c983be-ca09-419d-9dc5-32ebbed88afe',
'f59b7af2-4261-4164-a54f-a50fb9d34c5c',
'f629552e-a431-4548-b8ae-4826d60e2aea',
'fa87cd54-052f-4003-aefc-5cf6e6e8b541',
'fc4f084d-2d46-4a90-9ece-dad859d1aecf',
'fc604213-e624-4502-a48f-db3f1f1d7667',
'ff295785-2ace-4671-9a8b-a829fcef2f82',
'ff48214d-f4a1-47e3-8bb4-9ff82921c690',
'ff4c9c8c-e8e5-480b-81e6-91dd0a01cdbf');

-- Update school_tenant_id column for schools which is deleted in source and marked as status=4 in Redshift (Story: ALEF-9441)
update devalefdw.dim_school
set school_tenant_id='93e4949d-7eff-4707-9201-dac917a5e013'
where school_status=4 and school_organisation='MOE';

-- Update school_alias column (Story: ALEF-9641)
update alefdw.dim_school set school_alias = sa.alias from qa_alefdw.school_alias sa where sa.uuid = school_id;

-- Update orphan learning_path(level) for subjects which is deleted in source but learning_path(level) are not deleted in Redshift (Story: ALEF-10056)
update devalefdw.dim_learning_path
    set learning_path_status=4
where learning_path_id in
      (
       select lp.learning_path_id
       from devalefdw.dim_subject s
       inner join devalefdw.dim_learning_path lp
       on s.subject_id=lp.learning_path_subject_id
       where s.subject_status=4 and lp.learning_path_status=1
      );

-- Delete LO which is deleted in source manually to fix content data. Result of User mistake for wrong content order (Story: ALEF-10056)
update alefdw.dim_learning_objective
set lo_status=4
where lo_id in(
'b2f1c1b0-e689-473d-a361-d5e6fe58bf81',
'89be99fe-a22d-4ed7-9f2b-9c8ab455f1cb',
'd3401f97-0a77-40e0-8ede-88539f9a8b5f',
'a3455e7e-3055-455c-8616-f0c2a1989519',
'4cf78dfb-b856-49c0-821f-113ec731e0ac',
'34c21a67-2487-467b-be27-93df27307e33',
'e0f88ea6-7851-4fe5-974b-4b2d7023eeb5',
'c7fc2ff0-497e-42f5-8eb9-85ad97da0496');

insert into alefdw.dim_learning_path(learning_path_created_time,
                                     learning_path_updated_time,
                                     learning_path_deleted_time,
                                     learning_path_dw_created_time,
                                     learning_path_dw_updated_time,
                                     learning_path_status,
                                     learning_path_id,
                                     learning_path_uuid,
                                     learning_path_name,
                                     learning_path_lp_status,
                                     learning_path_language_type_script,
                                     learning_path_experiential_learning,
                                     learning_path_tutor_dhabi_enabled,
                                     learning_path_default,
                                     learning_path_school_id,
                                     learning_path_subject_id,
                                     learning_path_curriculum_id,
                                     learning_path_curriculum_grade_id,
                                     learning_path_curriculum_subject_id)

values
( sysdate, null, null, sysdate, null, 1, '7684add4-7f35-47fb-8b49-b6bbea646c76'||'8e4e65b9-ba15-4590-9e46-cac0c77b7b4d'||'dc163645-36ba-499e-a7ec-dbda5d95b35d',
         '7684add4-7f35-47fb-8b49-b6bbea646c76',
         'Group 1',
         'ONLINE',
         'L-T-R Language',
         true,
         false,
         true,
         '8e4e65b9-ba15-4590-9e46-cac0c77b7b4d',
         'dc163645-36ba-499e-a7ec-dbda5d95b35d',
         392027,
         596550,
         571671),
( sysdate, null,
         null,
         sysdate,
         null,
         1,
         '30f6eb3c-52f5-4bb4-b931-5ccd3aa2ec7c'||'55969308-d2b6-4164-917b-c00300f7d066'||'40ea96bf-6a66-4664-a9a1-7ab7273d4ae1',
         '30f6eb3c-52f5-4bb4-b931-5ccd3aa2ec7c',
         '1',
         'ONLINE',
         'L-T-R Language',
         true,
         true,
         true,
         '55969308-d2b6-4164-917b-c00300f7d066',
         '40ea96bf-6a66-4664-a9a1-7ab7273d4ae1',
         392027,
         768780,
         742980),
( sysdate, null,
         null,
         sysdate,
         null,
         1,
         '081b177d-fd97-4eac-a94f-661b831a3f6d'||'2c38e83f-b24c-43d7-8454-c68f36e7d661'||'fa7f6d7f-96a9-4c7f-9547-97eee57a1fff',
         '081b177d-fd97-4eac-a94f-661b831a3f6d',
         'Group 1',
         'ONLINE',
         'R-T-L Language',
         true,
         false,
         true,
         '2c38e83f-b24c-43d7-8454-c68f36e7d661',
         'fa7f6d7f-96a9-4c7f-9547-97eee57a1fff',
         392027,
         596550,
         352071),
( sysdate,
 null,
         null,
         sysdate,
         null,
         1,
         'baa81008-ca06-4610-bd7a-3297531bdf8f'||'c288cfee-83b6-4d6f-9d5a-ee73d905b80e'||'4e8670de-f2f6-4339-996c-28c10dba744b',
         'baa81008-ca06-4610-bd7a-3297531bdf8f',
         '1',
         'ONLINE',
         'R-T-L Language',
         true,
         false,
         true,
         'c288cfee-83b6-4d6f-9d5a-ee73d905b80e',
         '4e8670de-f2f6-4339-996c-28c10dba744b',
         392027,
         322135,
         934714),
( sysdate, null,
         null,
         sysdate,
         null,
         1,
         '30f6eb3c-52f5-4bb4-b931-5ccd3aa2ec7c'||'b7fa68a7-1c0e-426b-bf55-2cc309915e86'||'c625c7b7-c9e2-4267-8d3a-d5bb3d24cbc3',
         '30f6eb3c-52f5-4bb4-b931-5ccd3aa2ec7c',
         '1',
         'ONLINE',
         'L-T-R Language',
         true,
         true,
         true,
         'b7fa68a7-1c0e-426b-bf55-2cc309915e86',
         'c625c7b7-c9e2-4267-8d3a-d5bb3d24cbc3',
         392027,
         768780,
         742980)
( sysdate, null, null, sysdate, null, 1,
  'ba0ba2c9-2482-47e9-9bf0-62db008e3fdb'||'a0e99be7-7772-4d90-9015-e9d81a956bd7'||'8f2bed7c-bce6-46a6-a65e-4c63bb6f3767',
         'ba0ba2c9-2482-47e9-9bf0-62db008e3fdb',
         '1',
         'ONLINE',
         'L-T-R Language',
         true,
         true,
         true,
         'a0e99be7-7772-4d90-9015-e9d81a956bd7',
         '8f2bed7c-bce6-46a6-a65e-4c63bb6f3767',
         392027,
         322135,
         571671)

    ,
( sysdate, null, null, sysdate, null, 1,
  '0c7aff36-8910-4a7b-b45a-cf2a78c7396f'||'f2cd9c55-90cc-43b0-9481-f3fe166475b0'||'a62ae607-c603-4bb9-bf9a-250e652500a1',
         '0c7aff36-8910-4a7b-b45a-cf2a78c7396f',
         'Group 1',
         'ONLINE',
         'L-T-R Language',
         true,
         true,
         true,
         'f2cd9c55-90cc-43b0-9481-f3fe166475b0',
         'a62ae607-c603-4bb9-bf9a-250e652500a1',
         392027,
         596550,
         742980),
  ( sysdate, null, null, sysdate, null, 1,
  'daf3bc12-0b8d-43e7-a922-399c13df2452'||'72b2c783-81d0-432a-8024-b612e4a4d0d4'||'cea8c717-21a6-4b43-941a-dbe86f317032',
         'daf3bc12-0b8d-43e7-a922-399c13df2452',
         'Group 1_1',
         'ONLINE',
         'L-T-R Language',
         true,
         false,
         true,
         '72b2c783-81d0-432a-8024-b612e4a4d0d4',
         'cea8c717-21a6-4b43-941a-dbe86f317032',
         563622,
         322135,
         571671),
  ( sysdate, null, null, sysdate, null, 1,
  '7684add4-7f35-47fb-8b49-b6bbea646c76'||'1aebf2a9-dc77-46da-b64d-72ee10eeac24'||'6c39d54d-4fda-4906-ad57-038d8266d673',
         '7684add4-7f35-47fb-8b49-b6bbea646c76',
         '1',
         'ONLINE',
         'L-T-R Language',
         true,
         true,
         true,
         '1aebf2a9-dc77-46da-b64d-72ee10eeac24',
         '6c39d54d-4fda-4906-ad57-038d8266d673',
         392027,
         596550,
         571671),
        ( sysdate, null, null, sysdate, null, 1,
  '081b177d-fd97-4eac-a94f-661b831a3f6d'||'6dafa1e4-6183-4cf4-bbff-3bc21b92feb7'||'74c804bf-37f8-49ce-bdd8-ff173ae8ad82',
         '081b177d-fd97-4eac-a94f-661b831a3f6d',
         '1',
         'ONLINE',
         'L-T-R Language',
         true,
         true,
         true,
         '6dafa1e4-6183-4cf4-bbff-3bc21b92feb7',
         '74c804bf-37f8-49ce-bdd8-ff173ae8ad82',
         392027,
         596550,
         571671),
        ( sysdate, null, null, sysdate, null, 1,
  'd3fbc553-4295-4867-ae76-51f0a01f65f7'||'1aebf2a9-dc77-46da-b64d-72ee10eeac24'||'1bb1e840-0b3f-4645-a194-d9728aff312a',
         'd3fbc553-4295-4867-ae76-51f0a01f65f7',
         '1',
         'ONLINE',
         'R-T-L Language',
         true,
         false,
         true,
         '1aebf2a9-dc77-46da-b64d-72ee10eeac24',
         '1bb1e840-0b3f-4645-a194-d9728aff312a',
         392027,
         596550,
         658224),
       ( sysdate, null, null, sysdate, null, 1,
  '5610c4fe-2de3-4d87-9e24-1bc97aac1900'||'10309e9c-93b9-47a0-b47d-915102fe1859'||'0d3092ab-6cda-48b1-8550-8628950f988f',
         '5610c4fe-2de3-4d87-9e24-1bc97aac1900',
         '1',
         'ONLINE',
         'L-T-R Language',
         true,
         false,
         true,
         '10309e9c-93b9-47a0-b47d-915102fe1859',
         '0d3092ab-6cda-48b1-8550-8628950f988f',
         392027,
         596550,
         186926),
        ( sysdate, null, null, sysdate, null, 1,
  '3e7b10e3-bd82-4969-8220-c19b5f634b5b'||'a0e99be7-7772-4d90-9015-e9d81a956bd7'||'a7cb4afc-a229-4229-a665-44e38b6b2b31',
         '3e7b10e3-bd82-4969-8220-c19b5f634b5b',
         '1',
         'ONLINE',
         'L-T-R Language',
         true,
         true,
         true,
         'a0e99be7-7772-4d90-9015-e9d81a956bd7',
         'a7cb4afc-a229-4229-a665-44e38b6b2b31',
         392027,
         322135,
         742980)

update alefdw.dim_learning_objective
set lo_status       = 4
where lo_id = '2e414c66-b272-44fa-907e-37d48e0ac0a0'
and lo_code = 'EN10_MLO_001'

update alefdw.dim_guardian
set guardian_status       = 2,
    guardian_active_until = t.guardian_created_time
from (select g.guardian_dw_id, g1.guardian_created_time
      from alefdw.dim_guardian g
             join alefdw.dim_guardian g1 on g.guardian_dw_id = g1.guardian_dw_id
      where g.guardian_status = 1
        and g1.guardian_status = 4
        and g.guardian_active_until isnull
        and g1.guardian_active_until isnull) t
where guardian_test.guardian_dw_id = t.guardian_dw_id
  and guardian_test.guardian_status = 1
  and guardian_test.guardian_active_until isnull;

update alefdw.dim_guardian
set guardian_dw_id = u.user_dw_id
from alefdw_stage.rel_user u
where guardian_id = u.user_id
  and guardian_status = 4 and guardian_dw_id isnull


-- Update orphan learning_path(level) for subjects which is deleted in source but learning_path(level) are not deleted in Redshift (Story: ALEF-10056)
update alefdw.dim_learning_path
    set learning_path_status=4
where learning_path_id in
      (
       select lp.learning_path_id
       from alefdw.dim_subject s
       inner join alefdw.dim_learning_path lp
       on s.subject_id=lp.learning_path_subject_id
       where s.subject_status=4 and lp.learning_path_status=1
      );

-- Delete LO which is deleted in source manually to fix content data. Result of User mistake for wrong content order (Story: ALEF-10056)
update alefdw.dim_learning_objective
set lo_status=4
where lo_id in(
'b2f1c1b0-e689-473d-a361-d5e6fe58bf81',
'89be99fe-a22d-4ed7-9f2b-9c8ab455f1cb',
'd3401f97-0a77-40e0-8ede-88539f9a8b5f',
'a3455e7e-3055-455c-8616-f0c2a1989519',
'4cf78dfb-b856-49c0-821f-113ec731e0ac',
'34c21a67-2487-467b-be27-93df27307e33',
'e0f88ea6-7851-4fe5-974b-4b2d7023eeb5',
'c7fc2ff0-497e-42f5-8eb9-85ad97da0496');

-- Updating MLO title as source updated it manually (Story: ALEF-10631)
update alefdw.dim_learning_objective
set lo_title ='You and your Family (2)'
where lo_id='c52c04dd-8cc0-4112-bbfc-20a96a0bf088'

-- Backfill academic year id to learning path from academic year (Story: ALEF-10523)

/*
1. Take school service backup (download csv result) with following query to get the current academic year of school+subject combination

SELECT s.id, s.school_uuid, s2.current_year_id, a.start_date, a.end_date, 'moe' tenant
from alef_school_moe.subject s
       join alef_school_moe.school s2 on s.school_uuid = s2.uuid
       join alef_school_moe.academic_year a on s2.current_year_id = a.id
union
SELECT s.id, s.school_uuid, s2.current_year_id, a.start_date, a.end_date, 'private' tenant
from alef_school_private.subject s
       join alef_school_private.school s2 on s.school_uuid = s2.uuid
       join alef_school_private.academic_year a on s2.current_year_id = a.id
union
SELECT s.id, s.school_uuid, s2.current_year_id, a.start_date, a.end_date, 'us' tenant
from alef_school_us.subject s
       join alef_school_us.school s2 on s.school_uuid = s2.uuid
       join alef_school_us.academic_year a on s2.current_year_id = a.id;

2. Copy backup to s3
- rsync -r <path of backup> hadoop@####.com:/home/hadoop/<destination-path>

3. Login to emr
-  aws s3 cp /home/hadoop/alef-data-platform/<backup-path> s3://<destination-path>

4. create this backup table in qa_alefdw
CREATE TABLE school_academic_yr (
  id VARCHAR,
  school_uuid VARCHAR,
  current_year_id VARCHAR,
  start_date DATETIME,
  end_date DATETIME,
  tenant VARCHAR
);

5. copy backup data to backup table in qa_alefdw
copy qa_alefdw.school_academic_yr(id,school_uuid,current_year_id,start_date,end_date,tenant)
from 's3://<backup-csv-path>'
credentials 'aws_access_key_id=####;aws_secret_access_key=####'
format csv
ignoreheader 1;
*/

update alefdw.dim_learning_path
set learning_path_academic_year_id = ay.current_year_id
from qa_alefdw.school_academic_yr ay
where dim_learning_path.learning_path_school_id+dim_learning_path.learning_path_subject_id = ay.school_uuid+ay.id;

update alefdw.dim_learning_path
set learning_path_academic_year_id = ay.academic_year_id
from (select *
      from alefdw.dim_academic_year
      where academic_year_school_id + academic_year_id not in
            (select learning_path_school_id + learning_path_academic_year_id
             from alefdw.dim_learning_path
             where learning_path_academic_year_id notnull)) ay
where dim_learning_path.learning_path_school_id = ay.academic_year_school_id
  and learning_path_academic_year_id isnull;

-- Updating Tenant name from HCZ to US as more schools will be added in US other than HCZ
update alefdw.dim_tenant
    set tenant_name='US'
where tenant_dw_id=3;

insert into alefdw_stage.rel_user (user_id) values ('DEFAULT_ID');

-- Updating MLO title (Story: ALEF-11043)
update alefdw.dim_learning_objective
set lo_title ='Show My Learning - 4'
where lo_id='be4ed692-2700-4dc4-a8ac-7525968bb9be';


------------------------------------------------------------ * ---------------------------------------------------------
-- Backfill dim_step_instance
-- copy content to dim_step_instance
insert into alefdw.dim_step_instance (
    step_instance_lo_id,
    step_instance_created_time,
    step_instance_updated_time,
    step_instance_dw_created_time,
    step_instance_dw_updated_time,
    step_instance_status,
    step_instance_step_id,
    step_instance_lo_ccl_id,
    step_instance_id,
    step_instance_attach_status,
    step_instance_type
)
select
    lo_association_lo_id,
    lo_association_created_time,
    lo_association_updated_time,
    lo_association_dw_created_time,
    lo_association_dw_updated_time,
    lo_association_status,
    lo_association_step_id,
    lo_association_lo_ccl_id,
    lo_association_id,
    lo_association_attach_status,
    lo_association_type
from alefdw.dim_learning_objective_association
where lo_association_type = 1;

-- delete content that were copied on previous step
delete from alefdw.dim_learning_objective_association where lo_association_type = 1;

-- update fle_step_id for fact_learning_experience from backups.content_step table

update alefdw.fact_learning_experience
    set fle_step_id = fle_content_id;

update alefdw.fact_learning_experience
    set fle_step_id = cs.newcontentuuid
    from backups.content_step cs
    where cs.oldcontenuuid = fle_step_id;

update alefdw.fact_practice
    set practice_item_step_id = practice_item_content_id;

update alefdw.fact_practice
    set practice_item_step_id = cs.newcontentuuid
    from backups.content_step cs
    where cs.oldcontenuuid = practice_item_step_id;

update alefdw.fact_practice_session
    set practice_session_item_step_id = practice_session_item_content_uuid;

update alefdw.fact_practice_session
    set practice_session_item_step_id = cs.newcontentuuid
    from backups.content_step cs
    where cs.oldcontenuuid = practice_session_item_step_id;

update alefdw.dim_content_student_association
    set content_student_association_step_id = content_student_association_content_id;

update alefdw.dim_content_student_association
    set content_student_association_step_id = cs.newcontentuuid
    from backups.content_step cs
    where cs.oldcontenuuid = content_student_association_step_id;
------------------------------------------------------------ * ---------------------------------------------------------
------------------------------------ MIGRATION OF ORGANISATION ID -----------------------------------------------
-------------- ON SCHOOL TABLE -------------------
--Redshift
UPDATE alefdw.dim_school
SET school_organisation_dw_id = o.organisation_dw_id
FROM alefdw.dim_organisation o
       join alefdw.dim_school s on s.school_organisation = o.organisation_name;

-------------- ON INSTRUCTIONAL PLAN TABLE -------------------
--Redshift
UPDATE alefdw.dim_instructional_plan
SET instructional_plan_organisation_dw_id = o.organisation_dw_id
FROM alefdw.dim_organisation o
       join alefdw.dim_instructional_plan s on s.instructional_plan_organisation_id = o.organisation_id;

-------------- ON TERM TABLE -------------------
--Redshift
UPDATE alefdw.dim_term
SET term_dw_id = o.organisation_dw_id
FROM alefdw.dim_term o
       join alefdw.dim_term s on s.term_organisation_id = o.organisation_id;

------------------------------------------------------------ * ---------------------------------------------------------

update alefdw.dim_step_instance set step_instance_pool_id = step_instance_id where step_instance_type = 5;

------------------------------------------------------------ * ---------------------------------------------------------

--******************************************TO BE RELEASED**************************************************************

------------------------------------ BACKFILLING OF ORGANISATION COLUMN ------------------------------------------------
-------------- ON CONTENT TABLE -------------------
--REDSHIFT--
UPDATE alefdw.dim_content
SET content_organisation = 'shared'
WHERE 1=1;

-------------- ON CURRICULUM TABLE -------------------
--REDSHIFT--
UPDATE alefdw.dim_curriculum
SET curr_organisation = 'shared'
WHERE 1=1;

-------------- ON LEARNING_OBJECTIVE TABLE -------------------
--REDSHIFT--
UPDATE alefdw.dim_learning_objective
SET lo_organisation = 'shared'
WHERE 1=1;

-------------- ON LESSON_TEMPLATE TABLE -------------------
--REDSHIFT--
UPDATE alefdw.dim_template
SET template_organisation = 'shared'
WHERE 1=1;

------------------------------------------------------------ * ---------------------------------------------------------
------------------------------------ BACKFILLING OF FLEXIBLE LESSON COLUMNS --------------------------------------------
--REDSHIFT--
-------------- ON learning_experience TABLES -------------------
update devalefdw.fact_learning_experience
set
    fle_activity_type           = fle.fle_lesson_category,
    fle_activity_template_id    = lo_type,
    fle_abbreviation            = fle.fle_lesson_type,
     fle_activity_component_type = case
                                     when fle.fle_lesson_type like 'TEQ%' or
                                     fle.fle_lesson_type = 'SA' or
                                     fle.fle_lesson_type = 'ASGN' then 'ASSIGNMENT'
                                     else case
                                            when fle.fle_lesson_type like 'KT' then 'KEY_TERM'
                                            else 'CONTENT'
             END
         END,
    fle_exit_ticket             = case when fle.fle_lesson_type = 'SA' then true else false END,
    fle_completion_node         = case when fle.fle_lesson_type = 'SA' then true else false END,
    fle_main_component = case
                           when fle.fle_lesson_type = 'MP' or
                           fle.fle_lesson_type = 'R_2' or
                           fle.fle_lesson_type = 'R_2' or
                           fle.fle_lesson_type = 'R_3' then false
                           else true END
from devalefdw.fact_learning_experience fle
       join devalefdw.dim_learning_objective lo on fle.fle_lo_dw_id = lo.lo_dw_id;

update devalefdw_stage.staging_learning_experience
set fle_activity_type           = fle.fle_lesson_category,
    fle_activity_template_id    = lo_type,
    fle_abbreviation            = fle.fle_lesson_type,
    fle_activity_component_type = case
                                    when fle.fle_lesson_type like 'TEQ%' or
                                    fle.fle_lesson_type = 'SA' or
                                    fle.fle_lesson_type = 'ASGN' then 'ASSIGNMENT'
                                    else case
                                           when fle.fle_lesson_type like 'KT' then 'KEY_TERM'
                                           else 'CONTENT'
            END
        END,
    fle_exit_ticket             = case when fle.fle_lesson_type = 'SA' then true else false END,
    fle_completion_node         = case
                                    when fle.fle_lesson_type = 'MP' or
                                         fle.fle_lesson_type = 'R' or
                                         fle.fle_lesson_type = 'R_2' or
                                         fle.fle_lesson_type = 'R_3'
                                            then false
                                    ELSE true END
from devalefdw_stage.staging_learning_experience fle
       join devalefdw.dim_learning_objective lo on fle.lo_uuid = lo.lo_id;

-------------- ON experience_submitted TABLES -------------------
update devalefdw.fact_experience_submitted
set fes_activity_type           = fes_lesson_category,
    fes_abbreviation            = fes_lesson_type,
    fes_activity_component_type = case
                                    when fes_lesson_type like 'TEQ%' or fes_lesson_type = 'SA' or fes_lesson_type =
                                         'ASGN' then 'ASSIGNMENT'
                                    else case
                                           when fes_lesson_type like 'KT' then 'KEY_TERM'
                                           else 'CONTENT'
            END
        END,
    fes_exit_ticket             = case when fes_lesson_type = 'SA' then true else false END,
    fes_completion_node         = case
                                    when fes_lesson_type = 'MP' or
                                         fes_lesson_type = 'R' or
                                         fes_lesson_type = 'R_2' or
                                         fes_lesson_type = 'R_3'
                                            then false
                                    ELSE true END;

update devalefdw_stage.staging_experience_submitted
set fes_activity_type           = fes_lesson_category,
    fes_abbreviation            = fes_lesson_type,
    fes_activity_component_type = case
                                    when fes_lesson_type like 'TEQ%' or fes_lesson_type = 'SA' or fes_lesson_type =
                                         'ASGN' then 'ASSIGNMENT'
                                    else case
                                           when fes_lesson_type like 'KT' then 'KEY_TERM'
                                           else 'CONTENT'
            END
        END,
    fes_exit_ticket             = case when fes_lesson_type = 'SA' then true else false END,
    fes_completion_node         = case
                                    when fes_lesson_type = 'MP' or
                                         fes_lesson_type = 'R' or
                                         fes_lesson_type = 'R_2' or
                                         fes_lesson_type = 'R_3'
                                            then false
                                    ELSE true END;
------------------------------------------------------------ * ---------------------------------------------------------
------------------------------------ BACKFILLING OF ASSIGNMENT INSTANCE STUDENT ASSOCIATION TABLE --------------------------------------------
--REDSHIFT--
-- QUERY TO MIGRATE STUDENT ASSOCIATION FROM dim_assignment and rel table TO STAGING rel_assignment_instance_student --
INSERT INTO alefdw_stage.rel_assignment_instance_student (
	ais_created_time,
	ais_dw_created_time,
	ais_updated_time,
	ais_dw_updated_time,
	ais_deleted_time,
	ais_status,
	ais_instance_id,
	ais_student_id
)
SELECT
	assignment_instance_created_time 		AS 	ais_created_time,
	assignment_instance_dw_created_time 	AS 	ais_dw_created_time,
	assignment_instance_updated_time 		AS	ais_updated_time,
	assignment_instance_dw_updated_time 	AS 	ais_dw_updated_time,
	assignment_instance_deleted_time 		AS 	ais_deleted_time,
	assignment_instance_status 				AS 	ais_status,
	assignment_instance_id 					AS  ais_instance_id,
	assignment_instance_student_id 			AS 	ais_student_id
FROM alefdw.dim_assignment_instance
UNION ALL
SELECT
	assignment_instance_created_time	AS 	ais_created_time,
	assignment_instance_dw_created_time	AS 	ais_dw_created_time,
	assignment_instance_updated_time	AS	ais_updated_time,
	assignment_instance_dw_updated_time	AS 	ais_dw_updated_time,
	assignment_instance_deleted_time	AS 	ais_deleted_time,
	assignment_instance_status			AS 	ais_status,
	assignment_instance_id				AS  ais_instance_id,
	assignment_instance_student_id		AS  ais_student_id
FROM alefdw_stage.rel_assignment_instance;

--- DELETE DUPLICATES FROM alefdw.dim_assignment_instance ---
DELETE FROM alefdw.dim_assignment_instance
USING (
	SELECT id FROM (
		SELECT
	        id,
	        ROW_NUMBER() OVER (PARTITION BY assignment_instance_id ORDER BY assignment_instance_created_time) AS row_num
	    FROM (
	        SELECT assignment_instance_dw_id as id, assignment_instance_id, assignment_instance_created_time FROM alefdw.dim_assignment_instance
	        UNION ALL
	        SELECT assignment_instance_staging_id as rel_id, assignment_instance_id, assignment_instance_created_time FROM alefdw_stage.rel_assignment_instance
	     )
	) WHERE row_num != 1
) ai_duplicates
WHERE
	assignment_instance_dw_id = ai_duplicates.id;

--- DELETE DUPLICATES FROM alefdw_stage.rel_assignment_instance ---
DELETE FROM alefdw_stage.rel_assignment_instance
USING (
	SELECT id FROM (
		SELECT
	        id,
	        ROW_NUMBER() OVER (PARTITION BY assignment_instance_id ORDER BY assignment_instance_created_time) AS row_num
	    FROM (
		    SELECT assignment_instance_staging_id as id, assignment_instance_id, assignment_instance_created_time FROM alefdw_stage.rel_assignment_instance
		    UNION ALL
	        SELECT assignment_instance_dw_id as dim_id, assignment_instance_id, assignment_instance_created_time FROM alefdw.dim_assignment_instance
	     )
	) WHERE row_num != 1
) ai_duplicates
WHERE
	assignment_instance_staging_id = ai_duplicates.id;
------------------------------------------------------------ * ---------------------------------------------------------
-------------------------------- BACKFILLING OF INSTRUCTIONAL PLAN NEW COLUMNS -----------------------------------------
UPDATE alefdw.dim_instructional_plan
SET instructional_plan_item_instructor_led=true,
SET instructional_plan_item_default_locked=true
WHERE 1=1;

UPDATE alefdw_stage.rel_instructional_plan
SET instructional_plan_item_instructor_led=true,
SET instructional_plan_item_default_locked=true
WHERE 1=1;
------------------------------------------------------------ * ---------------------------------------------------------
-------------------------------- BACKFILLING OF ACADEMIC YEAR is_roll_over_completed COLUMN ----------------------------
UPDATE
    alefdw.dim_academic_year
SET academic_year_is_roll_over_completed = true
FROM (
         SELECT ay.*,
                ROW_NUMBER() OVER (PARTITION BY academic_year_school_id ORDER BY academic_year_end_date DESC) AS rn
         FROM alefdw.dim_academic_year AS ay
     ) ranked_ay
WHERE ranked_ay.rn <> 1
  AND ranked_ay.academic_year_id = dim_academic_year.academic_year_id
  AND ranked_ay.academic_year_school_id = dim_academic_year.academic_year_school_id;
------------------------------------------------------------ * ---------------------------------------------------------
----------------------------- Making dim class as scd -------------------------------------------------------------------

create table alefdw.temp_dim_class
(
    rel_class_dw_id bigint identity (1,1),
    class_dw_id bigint,
    class_created_time timestamp,
    class_updated_time timestamp,
    class_deleted_time timestamp,
    class_dw_created_time timestamp,
    class_dw_updated_time timestamp,
    class_status int,
    class_id varchar(36),
    class_title varchar(255),
    class_school_id varchar(36),
    class_grade_id varchar(36),
    class_section_id varchar(36),
    class_academic_year_id varchar(36),
    class_gen_subject varchar(255) ,
    class_curriculum_id bigint,
    class_curriculum_grade_id bigint,
    class_curriculum_subject_id bigint,
    class_content_academic_year int,
    class_tutor_dhabi_enabled boolean,
    class_language_direction varchar(25),
    class_online boolean,
    class_practice boolean,
    class_course_status varchar(50),
    class_source_id varchar(255),
    class_curriculum_instructional_plan_id varchar(36),
    class_category_id varchar(36),
    class_active_until timestamp
    )
    diststyle all
    sortkey(rel_class_dw_id);
INSERT INTO alefdw.temp_dim_class
(class_dw_id,
 class_created_time,
 class_updated_time,
 class_deleted_time,
 class_dw_created_time,
 class_dw_updated_time,
 class_status,
 class_id,
 class_title,
 class_school_id,
 class_grade_id,
 class_section_id,
 class_academic_year_id,
 class_gen_subject,
 class_curriculum_id,
 class_curriculum_grade_id,
 class_curriculum_subject_id,
 class_content_academic_year,
 class_tutor_dhabi_enabled,
 class_language_direction,
 class_online,
 class_practice,
 class_course_status,
 class_source_id,
 class_curriculum_instructional_plan_id,
 class_category_id,
 class_active_until) select
                         class_dw_id,
                         class_created_time,
                         class_updated_time,
                         class_deleted_time,
                         class_dw_created_time,
                         class_dw_updated_time,
                         class_status,
                         class_id,
                         class_title,
                         class_school_id,
                         class_grade_id,
                         class_section_id,
                         class_academic_year_id,
                         class_gen_subject,
                         class_curriculum_id,
                         class_curriculum_grade_id,
                         class_curriculum_subject_id,
                         class_content_academic_year,
                         class_tutor_dhabi_enabled,
                         class_language_direction,
                         class_online,
                         class_practice,
                         class_course_status,
                         class_source_id,
                         class_curriculum_instructional_plan_id,
                         class_category_id,
                         class_active_until from alefdw.dim_class;

CREATE TABLE alefdw.backup_dim_class AS SELECT * FROM alefdw.dim_class;
DROP TABLE alefdw.dim_class;
ALTER TABLE  alefdw.temp_dim_class rename to dim_class;


UNLOAD ('select class_dw_id, class_id from alefdw.dim_class') to 's3://alef-bigdata-emr/alef-data-platform/alef-23027/' CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
COPY alefdw_stage.rel_dw_id_mappings from 's3://alef-bigdata-emr/alef-data-platform/alef-23027/' CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' EXPLICIT_IDS CSV;

UPDATE alefdw.dim_class_category
SET class_category_status = 1
WHERE class_category_status is null;

UPDATE alefdw_stage.rel_dw_id_mappings set entity_type='class';


update alefdw_stage.rel_class_user set class_user_attach_status = class_user_status;
update alefdw.dim_class_user set class_user_attach_status = class_user_status;

update alefdw_stage.rel_class_user set class_user_status = 2 where class_user_active_until is not null;
update alefdw_stage.rel_class_user set class_user_status = 1 where class_user_active_until is null;

update alefdw.dim_class_user set class_user_status = 2 where class_user_active_until is not null;
update alefdw.dim_class_user set class_user_status = 1 where class_user_active_until is null;


update alefdw.dim_skill_association set skill_association_is_previous = false;

-- backfill experience_submitted fes_ls_id and exp_uuid columns
update alefdw.fact_experience_submitted set fes_ls_id = fle.fle_ls_id, exp_uuid = fle.fle_exp_id
from alefdw.fact_experience_submitted fes inner join alefdw.fact_learning_experience fle on fle.fle_dw_id = fes.fes_ls_dw_id
    and fle.fle_dw_id = fes.fes_exp_dw_id where fes.fes_ls_id is null and fes.exp_uuid is null;

-- backfill lesson_feedback lesson_feedback_fle_ls_uuid;
update alefdw.fact_lesson_feedback set lesson_feedback_fle_ls_uuid = fle.fle_ls_id
from alefdw.fact_lesson_feedback flf inner join alefdw.fact_learning_experience fle on fle.fle_dw_id = flf.lesson_feedback_fle_ls_dw_id
    and fle.fle_exp_ls_flag = false where flf.lesson_feedback_fle_ls_uuid is null;

-- backfill adt_student_report fasr_fle_ls_uuid
update alefdw.fact_adt_student_report set fasr_fle_ls_uuid = fle.fle_ls_id
from alefdw.fact_adt_student_report fasr inner join alefdw.fact_learning_experience fle on fle.fle_dw_id = fasr.fasr_fle_ls_uuid
    and fle.fle_exp_ls_flag = false where fasr.fasr_fle_ls_uuid is null;

-- backfill adt_student_report fanq_fle_ls_uuid
update alefdw.fact_adt_next_question set fanq_fle_ls_uuid = fle.fle_ls_id
from alefdw.fact_adt_next_question fanq inner join alefdw.fact_learning_experience fle on fle.fle_dw_id = fanq.fanq_fle_ls_dw_id
    and fle.fle_exp_ls_flag = false where fanq.fanq_fle_ls_uuid is null;

-- BACKFILL MATERIAL ID AND MATERIAL TYPE IN CLASS DIM -----------------------------------------------------------------
-- Redshift
update alefdw.dim_class
        set class_material_id = class_curriculum_instructional_plan_id,
         class_material_type = 'INSTRUCTIONAL_PLAN';

update alefdw_stage.rel_class
        set class_material_id = class_curriculum_instructional_plan_id,
         class_material_type = 'INSTRUCTIONAL_PLAN';


update alefdw_stage.staging_learning_experience set fle_material_id = fle_instructional_plan_id, fle_material_type = 'INSTRUCTIONAL_PLAN'
where fle_material_id is null;

update alefdw.fact_learning_experience set fle_material_id = fle_instructional_plan_id, fle_material_type = 'INSTRUCTIONAL_PLAN'
where fle_material_id is null;

update alefdw_stage.staging_experience_submitted set fes_material_id = fes_instructional_plan_id, fes_material_type = 'INSTRUCTIONAL_PLAN'
where fes_material_id is null;

update alefdw.fact_experience_submitted set fes_material_id = fes_instructional_plan_id, fes_material_type = 'INSTRUCTIONAL_PLAN'
where fes_material_id is null;

update alefdw.dim_interim_checkpoint set ic_material_type = 'INSTRUCTIONAL_PLAN'
where ic_material_type is null;

update alefdw_stage.rel_instructional_plan set instructional_plan_content_repository_id = instructional_plan_organisation_id
where instructional_plan_content_repository_id is null;

update alefdw.dim_instructional_plan set instructional_plan_content_repository_id = instructional_plan_organisation_id
where instructional_plan_content_repository_id is null;

update alefdw.dim_term set term_content_repository_dw_id = term_organisation_dw_id
where term_content_repository_dw_id is null;

update alefdw.dim_term set term_content_repository_id = term_organisation_id
where term_content_repository_id is null;

update alefdw.dim_content_repository set content_repository_status  = 1
where content_repository_status is null;

-- UPDATE step_instance_title to Exit Ticket for template ed0feb97-69f5-40d7-a298-e40a22791cd1 -------------------------
update alefdw.dim_step_instance set step_instance_title = 'Exit Ticket'
where step_instance_template_uuid = 'ed0feb97-69f5-40d7-a298-e40a22791cd1'
and step_instance_title = 'Level Up!';


-- backfill user_type and user_created_time for rel_user table
-- STUDENT UPDATE
update alefdw_stage.rel_user set user_type = 'STUDENT', user_created_time = ss.student_created_time
from alefdw_stage.rel_user u, (select s.student_created_time, ru.user_id
                               from alefdw_stage.rel_user ru
                                        join (select student_id, min(student_created_time) as student_created_time from alefdw.dim_student group by student_id) s
                                             on ru.user_id = s.student_id) ss where u.user_id = ss.user_id and u.user_type is null;


update alefdw_stage.rel_user set user_type = 'STUDENT', user_created_time = ss.student_created_time
from alefdw_stage.rel_user u, (select s.student_created_time, ru.user_id
                               from alefdw_stage.rel_user ru
                                        join (select student_uuid, min(student_created_time) as student_created_time from alefdw_stage.rel_student group by student_uuid) s
                                             on ru.user_id = s.student_uuid) ss where u.user_id = ss.user_id and u.user_type is null;


-- TEACHER UPDATE
update alefdw_stage.rel_user set user_type = 'TEACHER', user_created_time = tt.teacher_created_time
from alefdw_stage.rel_user u, (select t.teacher_created_time, ru.user_id
                               from alefdw_stage.rel_user ru
                                        join (select teacher_id, min(teacher_created_time) as teacher_created_time from alefdw.dim_teacher group by teacher_id) t
                                             on ru.user_id = t.teacher_id) tt where u.user_id = tt.user_id and u.user_type is null;


update alefdw_stage.rel_user set user_type = 'TEACHER', user_created_time = tt.teacher_created_time
from alefdw_stage.rel_user u, (select t.teacher_created_time, ru.user_id
                               from alefdw_stage.rel_user ru
                                        join (select teacher_uuid, min(teacher_created_time) as teacher_created_time from alefdw_stage.rel_teacher group by teacher_uuid) t
                                             on ru.user_id = t.teacher_uuid) tt where u.user_id = tt.user_id and u.user_type is null;


-- GUARDIAN UPDATE
update alefdw_stage.rel_user set user_type = 'GUARDIAN', user_created_time = gg.guardian_created_time
from alefdw_stage.rel_user u, (select g.guardian_created_time, ru.user_id
                               from alefdw_stage.rel_user ru
                                        join (select guardian_id, min(guardian_created_time) as guardian_created_time from alefdw.dim_guardian group by guardian_id) g
                                             on ru.user_id = g.guardian_id) gg where u.user_id = gg.user_id and u.user_type is null;

-- GUARDIAN UPDATE FOR STAGING
update alefdw_stage.rel_user set user_type = 'GUARDIAN', user_created_time = gg.guardian_created_time
from alefdw_stage.rel_user u, (select g.guardian_created_time, ru.user_id
                               from alefdw_stage.rel_user ru
                                        join (select guardian_uuid, min(guardian_created_time) as guardian_created_time from alefdw_stage.rel_guardian group by guardian_uuid) g
                                             on ru.user_id = g.guardian_uuid) gg where u.user_id = gg.user_id and u.user_type is null;


-- ADMIN UPDATE
update alefdw_stage.rel_user set user_type = 'ADMIN', user_created_time = aa.admin_created_time
from alefdw_stage.rel_user u, (select a.admin_created_time, ru.user_id
                               from alefdw_stage.rel_user ru
                                        join (select admin_id, min(admin_created_time) as admin_created_time from alefdw.dim_admin group by admin_id) a
                                             on ru.user_id = a.admin_id) aa where u.user_id = aa.user_id and u.user_type is null;


-- ADMIN UPDATE
update alefdw_stage.rel_user set user_type = 'ADMIN', user_created_time = aa.admin_created_time
from alefdw_stage.rel_user u, (select a.admin_created_time, ru.user_id
                               from alefdw_stage.rel_user ru
                                        join (select admin_uuid, min(admin_created_time) as admin_created_time from alefdw_stage.rel_admin group by admin_uuid) a
                                             on ru.user_id = a.admin_uuid) aa where u.user_id = aa.user_id and u.user_type is null;


update alefdw_stage.staging_announcement set fa_type = 2 where fa_type is null;
update alefdw.fact_announcement set fa_type = 2 where fa_type is null;



update alefdw.dim_pathway_level_activity_association set plaa_level_dw_id = t.dw_id
from alefdw.dim_pathway_level_activity_association plaa inner join alefdw_stage.rel_dw_id_mappings t
                                                                      on t.id = plaa.plaa_level_id and t.entity_type = 'pathway_level'
where plaa.plaa_level_dw_id is null;


update alefdw.dim_pathway_level_activity_association set plaa_activity_dw_id = t.act_dw_id
from alefdw.dim_pathway_level_activity_association plaa inner join
     (
         select lo_dw_id as act_dw_id, lo_id as act_id
         from alefdw.dim_learning_objective
         union
         select ic_dw_id as act_dw_id, ic_id as act_id
         from alefdw.dim_interim_checkpoint
     ) t on plaa.plaa_activity_id = t.act_id
where plaa.plaa_activity_dw_id is null;

update alefdw.dim_pathway_level_activity_association set plaa_pathway_id = t.pathway_level_pathway_id
from alefdw.dim_pathway_level_activity_association plaa inner join
    (
    select pathway_level_id, pathway_level_pathway_id from alefdw.dim_pathway_level group by pathway_level_id, pathway_level_pathway_id
    ) t on t.pathway_level_id = plaa.plaa_level_id
where plaa.plaa_pathway_id is null;

update alefdw.dim_pathway_level_activity_association set plaa_pathway_dw_id = t.pathway_dw_id
from alefdw.dim_pathway_level_activity_association plaa inner join
     (
         select pathway_id, pathway_dw_id from alefdw.dim_pathway group by pathway_id, pathway_dw_id
     ) t on t.pathway_id = plaa_pathway_id
where plaa.plaa_pathway_dw_id is null;

update alefdw.dim_instructional_plan set instructional_plan_content_repository_dw_id = cr.dw_id
from alefdw.dim_instructional_plan ip join
      testalefdw_stage.rel_dw_id_mappings cr on ip.instructional_plan_content_repository_id = cr.id
and cr.entity_type = 'content-repository';

update alefdw_stage.rel_instructional_plan set content_repository_uuid = instructional_plan_content_repository_id;

delete
  FROM alefdw.dim_pathway
  where rel_pathway_dw_id in (SELECT a.rel_pathway_dw_id
    FROM (SELECT *,
          row_number() over (partition by pathway_dw_id,
          pathway_id,
          pathway_status,
          pathway_name,
          pathway_code,
          pathway_subject_id,
          pathway_organization_dw_id,
          pathway_created_time,
          pathway_updated_time,
          pathway_lang_code
    order by
    pathway_dw_created_time) as rnk
  FROM testalefdw.dim_pathway) a
WHERE a.rnk > 1);


-- backfill ftc_session_state
update alefdw.fact_tutor_conversation set ftc_session_state = ftc.fts_session_state
from alefdw.fact_tutor_conversation ftc inner join
     (select fts_session_id, max(fts_session_state) as fts_session_state from alefdw.fact_tutor_session group by fts_session_id) fts on
             ftc.ftc_session_id = fts.fts_session_id;


-- migrate fact_service desk table with new dw_id approach
create table backups.fact_service_desk0910 as select * from alefdw.fact_service_desk_request;
drop table alefdw.fact_service_desk_request;
CREATE TABLE alefdw.fact_service_desk_request
(
    fsdr_dw_id BIGINT,
    fsdr_dw_created_time TIMESTAMP,
    fsdr_created_time TIMESTAMP,
    fsdr_completed_time TIMESTAMP,
    fsdr_category_name varchar(50),
    fsdr_sub_category varchar(50),
    fsdr_subject varchar(300),
    fsdr_request_status varchar(30),
    fsdr_request_type varchar(30),
    fsdr_request_id  BIGINT,
    fsdr_item_name varchar(30),
    fsdr_site_name varchar(50),
    fsdr_group_name varchar(50),
    fsdr_technician_name varchar(50),
    fsdr_template_name varchar(100),
    fsdr_closure_code_name varchar(50),
    fsdr_impact_name varchar(50),
    fsdr_resolved_time TIMESTAMP,
    fsdr_assigned_time TIMESTAMP,
    fsdr_sla_name varchar(50),
    fsdr_impact_details_name varchar(50),
    fsdr_linked_request_id BIGINT,
    fsdr_organization_name varchar(30),
    fsdr_is_escalated boolean,
    fsdr_priority_name varchar(50),
    fsdr_request_display_id BIGINT,
    fsdr_request_mode_name varchar(50),
    fsdr_requester_job_title varchar(200),
    fsdr_date_dw_id BIGINT
)
DISTSTYLE KEY DISTKEY (fsdr_dw_id)
SORTKEY (fsdr_created_time);

insert into table alefdw.fact_service_desk_request select * from backups.fact_service_desk0910;

insert into alefdatalakeproduction.product_max_ids (table_name, max_id, status, updated_time)
values ('fact_service_desk_request', 300089, 'completed', system_now());

-- CREATE SCHEMA FOR DATA_OFFSETS
val schema = new StructType()
.add(StructField("table_name", StringType))
.add(StructField("start_offset", TimestampType))
.add(StructField("end_offset", TimestampType))
.add(StructField("status", StringType))
.add(StructField("updated_time", TimestampType))

val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

df.printSchema
df.write.format("delta").mode("overwrite").save("s3a://alef-bigdata-emr/lakehouse/internal/data-offset")

create table alefdatalakeproduction.data_offset using delta location 's3://alef-bigdata-ireland/lakehouse/internal/data-offset/';

insert into alefdatalakeproduction.data_offset
(table_name, start_offset, end_offset, status, updated_time)
values
('fact_service_desk_request', '2021-08-01 02:55:29.605000', '2023-10-08 22:12:34.409000', system_now());
--update max time at backfilling


-- Back-filling dim_course_activity_association
INSERT INTO alefdw_stage.rel_course_activity_association (    caa_dw_id,
                                                              caa_created_time,
                                                              caa_updated_time,
                                                              caa_dw_created_time,
                                                              caa_dw_updated_time,
                                                              caa_status,
                                                              caa_attach_status,
                                                              caa_course_id,
                                                              caa_container_id,
                                                              caa_activity_id,
                                                              caa_activity_type,
                                                              caa_activity_pacing,
                                                              caa_activity_index,
                                                              caa_course_version,
                                                              caa_is_parent_deleted,
                                                              caa_grade,
                                                              caa_activity_is_optional,
                                                              caa_is_joint_parent_activity)
SELECT plaa_dw_id,
       plaa_created_time,
       plaa_updated_time,
       plaa_dw_created_time,
       plaa_dw_updated_time,
       plaa_status,
       plaa_attach_status,
       plaa_pathway_id,
       plaa_level_id,
       plaa_activity_id,
       plaa_activity_type,
       plaa_activity_pacing,
       plaa_activity_index,
       plaa_pathway_version,
       plaa_is_parent_deleted,
       plaa_grade,
       plaa_activity_is_optional,
       plaa_is_joint_parent_activity
FROM alefdw.dim_pathway_level_activity_association;

SELECT max(caa_dw_id)
FROM alefdw_stage.rel_course_activity_association;

insert into alefdw_stage.rel_course_activity_container_grade_association (cacga_dw_id,
                                                                          cacga_container_id,
                                                                          cacga_course_id,
                                                                          cacga_grade,
                                                                          cacga_created_time,
                                                                          cacga_dw_created_time,
                                                                          cacga_updated_time,
                                                                          cacga_status
)
select cacga_dw_id,
       cacga_container_id,
       cacga_course_id,
       cacga_grade,
       cacga_created_time,
       cacga_dw_created_time,
       cacga_updated_time,
       cacga_status
from (select ROW_NUMBER() OVER (ORDER BY pathway_level_created_time)         AS cacga_dw_id,
             pathway_level_id                                                        as cacga_container_id,
             pathway_level_pathway_id                                                as cacga_course_id,
             pathway_level_status                                                    as cacga_status,
             case
                 when pathway_level_attach_status = 4 then null
                 else pathway_level_grade end                                        as cacga_grade,
             pathway_level_created_time                                              as cacga_created_time,
             pathway_level_updated_time                                              as cacga_updated_time,
             current_timestamp                                                       as cacga_dw_created_time,
             row_number()
             over (partition by pathway_level_id,
                 pathway_level_pathway_id,
                 nvl(pathway_level_grade, ''),
                 pathway_level_created_time
                 order by pathway_level_created_time, pathway_level_dw_created_time) as rank
      from alefdw.dim_pathway_level
     )
where rank = 1
order by cacga_dw_id
;

select max(cacga_dw_id) from alefdw_stage.rel_course_activity_container_grade_association;


insert into alefdw_stage.rel_course_activity_container_domain (           cacd_dw_id,
                                                                          cacd_container_id,
                                                                          cacd_course_id,
                                                                          cacd_domain,
                                                                          cacd_sequence,
                                                                          cacd_created_time,
                                                                          cacd_dw_created_time,
                                                                          cacd_updated_time,
                                                                          cacd_status
)
select cacd_dw_id,
       cacd_container_id,
       cacd_course_id,
       cacd_domain,
       cacd_sequence,
       cacd_created_time,
       cacd_dw_created_time,
       cacd_updated_time,
       cacd_status
from (select 1 + ROW_NUMBER() OVER (ORDER BY pathway_level_created_time) - 1         AS cacd_dw_id,
              pathway_level_id                                                       as cacd_container_id,
             pathway_level_pathway_id                                                as cacd_course_id,
             pathway_level_status                                                    as cacd_status,
             case
                 when pathway_level_attach_status = 4 then null
                 else pathway_level_domain end                                        as cacd_domain,
             case
                 when pathway_level_attach_status = 4 then null
                 else pathway_level_sequence end                                        as cacd_sequence,
             pathway_level_created_time                                              as cacd_created_time,
             pathway_level_updated_time                                              as cacd_updated_time,
             current_timestamp                                                       as cacd_dw_created_time,
             row_number()
                 over (partition by pathway_level_id,
                 pathway_level_pathway_id,
                 nvl(pathway_level_domain, ''),
                 nvl(pathway_level_sequence, ''),
                 pathway_level_created_time
                 order by pathway_level_created_time, pathway_level_dw_created_time) as rank
      from alefdw.dim_pathway_level
      where pathway_level_id || pathway_level_pathway_id || nvl(pathway_level_domain, '') || nvl(pathway_level_sequence, '') in
            (select pathway_level_id || pathway_level_pathway_id || nvl(pathway_level_domain, '') || nvl(pathway_level_sequence, '')
             from alefdw.dim_pathway_level
             group by pathway_level_id, pathway_level_pathway_id, pathway_level_domain, pathway_level_sequence)
     )
where rank = 1
order by cacd_dw_id
;
select max(cacd_dw_id) from alefdw_stage.rel_course_activity_container_domain;

-- Back-filling dim_course_activity_association
INSERT INTO alefdw_stage.rel_course_activity_association (    caa_dw_id,
                                                              caa_created_time,
                                                              caa_updated_time,
                                                              caa_dw_created_time,
                                                              caa_dw_updated_time,
                                                              caa_status,
                                                              caa_attach_status,
                                                              caa_course_id,
                                                              caa_container_id,
                                                              caa_activity_id,
                                                              caa_activity_type,
                                                              caa_activity_pacing,
                                                              caa_activity_index,
                                                              caa_course_version,
                                                              caa_is_parent_deleted,
                                                              caa_grade,
                                                              caa_activity_is_optional,
                                                              caa_is_joint_parent_activity)
SELECT plaa_dw_id,
       plaa_created_time,
       plaa_updated_time,
       plaa_dw_created_time,
       plaa_dw_updated_time,
       plaa_status,
       plaa_attach_status,
       plaa_pathway_id,
       plaa_level_id,
       plaa_activity_id,
       plaa_activity_type,
       plaa_activity_pacing,
       plaa_activity_index,
       plaa_pathway_version,
       plaa_is_parent_deleted,
       plaa_grade,
       plaa_activity_is_optional,
       plaa_is_joint_parent_activity
FROM alefdw.dim_pathway_level_activity_association;

SELECT max(caa_dw_id)
FROM alefdw_stage.rel_course_activity_association;

-- Back-filling dim_course_activity_outcome_association
WITH ranked_plaa AS (SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY plaa_outcome_pathway_id, plaa_outcome_activity_id, nvl(plaa_outcome_id, 0), plaa_outcome_created_time
                                ORDER BY plaa_outcome_created_time, plaa_outcome_dw_created_time
                                ) AS rank
                     FROM alefdw.dim_plaa_outcome)
SELECT ROW_NUMBER() OVER (ORDER BY plaa_outcome_created_time) AS caoa_dw_id,
       plaa_outcome_created_time,
       plaa_outcome_updated_time,
       plaa_outcome_dw_created_time,
       plaa_outcome_dw_updated_time,
       plaa_outcome_status,
       plaa_outcome_pathway_id,
       plaa_outcome_activity_id,
       plaa_outcome_id,
       plaa_outcome_type,
       plaa_outcome_curr_id,
       plaa_outcome_curr_grade_id,
       plaa_outcome_curr_subject_id
FROM ranked_plaa
WHERE rank = 1;

SELECT max(caoa_dw_id)
FROM alefdw_stage.rel_course_activity_outcome_association;


-- start backfill rel_staff_user and rel_staff_user_school_role_association
insert into alefdw_stage.rel_staff_user (rel_staff_user_dw_id,
                                         staff_user_event_type,
                                         staff_user_created_time,
                                         staff_user_dw_created_time,
                                         staff_user_active_until,
                                         staff_user_status,
                                         staff_user_id,
                                         staff_user_onboarded,
                                         staff_user_avatar,
                                         staff_user_expirable,
                                         staff_user_exclude_from_report,
                                         staff_user_enabled
)
select  rel_admin_dw_id,
        'BACKFILLED',
        admin_created_time,
        admin_dw_created_time,
        admin_active_until,
        case
            when (admin_status = 3 and admin_active_until is null) or admin_status = 1 then 1
            when admin_status = 4 then 4
            else 2
            end as admin_status,
        admin_id,
        admin_onboarded,
        admin_avatar,
        admin_expirable,
        admin_exclude_from_report,
        case
            when admin_status = 3 then false
            when admin_status = 4 then false
            else true
            end as staff_user_enabled
from alefdw.dim_admin order by admin_created_time;


insert into alefdw_stage.rel_staff_user_school_role_association (susra_dw_id,
                                                                 susra_event_type,
                                                                 susra_staff_id,
                                                                 susra_school_id,
                                                                 susra_role_name,
                                                                 susra_role_uuid,
                                                                 susra_organization,
                                                                 susra_status,
                                                                 susra_created_time,
                                                                 susra_dw_created_time,
                                                                 susra_active_until)
select rel_admin_dw_id,
       'BACKFILLED',
       a.admin_id,
       s.school_id,
       r.role_name,
       r.role_uuid,
       null as susra_organization,
       case
           when a.admin_active_until is null then 1
           else 2
           end,
       a.admin_created_time,
       a.admin_dw_created_time,
       a.admin_active_until
from alefdw.dim_admin a
         left outer join alefdw.dim_school s on a.admin_school_dw_id = s.school_dw_id
         inner join alefdw.dim_role r on r.role_dw_id = a.admin_role_dw_id
order by admin_created_time;
-- end backfill rel_staff_user and rel_staff_user_school_role_association