SELECT  asset.assettitle      AS MAINTITLE,
               -- asset.s_parent_obj_id AS ParentObjdId,
                S_TEAM,
                S_INNINGS,
                S_BOWLINGORDER,
                S_BOWLER,
                S_OVERS,
                S_RUNS,
                S_WICKETS,
                S_MAIDEN,
                S_EXTRAS,
                S_NOBALL,
                S_WIDEBALL,
                S_ECONOMY,
                EX_MATCH_ID
FROM            bcciuc.mediaarchive.dam_assetbases asset
INNER JOIN      bcciuc.mediaarchive.dam_bcciucbowlingextn ext
ON              ext.id=asset.id
WHERE           asset.isdeleted=0