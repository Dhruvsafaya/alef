-- all existing avatars have to be updated as TRUE
UPDATE alefdw.dim_avatar SET avatar_is_enabled_for_all_orgs = true WHERE avatar_is_enabled_for_all_orgs IS NULL;
