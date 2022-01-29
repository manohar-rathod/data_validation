SELECT asset.assettitle      AS MAINTITLE,
       asset.s_parent_obj_id AS ParentObjdId,
                S_TEAM,
                S_INNINGS,
                S_FOW,
                S_BATTER,
                S_RUNS,
                S_OVERS,
                EX_MATCH_ID
FROM   bcciuc.mediaarchive.dam_assetbases asset
       INNER JOIN bcciuc.mediaarchive.dam_bcciucfowextn ext
               ON ext.id = asset.id
WHERE  asset.isdeleted = 0