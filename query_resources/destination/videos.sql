SELECT AssetTitle as maintitle,
       ex_old_dmid,
       ex_old_cfn,
       ex_match_id,
       s_created_on,
       s_session,
       s_day_num,
       s_innings,
       s_desc,
       s_comments
FROM   bcciuc.mediaarchive.dam_assetbases asset
       INNER JOIN bcciuc.mediaarchive.dam_bcciucvideosextn ext
               ON ext.id = asset.id
WHERE  asset.s_dm_id = 6897
       AND asset.isdeleted = 0