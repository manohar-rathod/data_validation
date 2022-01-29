SELECT asset.assettitle      AS MAINTITLE,
       --asset.s_parent_obj_id AS ParentObjId,
       S_TEAM,
       S_BAT_ORDER,
       S_BATTER,
       S_WICKETTYPE,
       S_FIELDERNAME,
       S_BOWLER,
       S_RUNS,
       S_MINUTES,
       S_FOURS,
       S_SIXES,
       S_STRIKERATE,
       S_BALLS,
       S_INNINGS,
       EX_MATCH_ID
FROM   bcciuc.mediaarchive.dam_assetbases asset
       INNER JOIN bcciuc.mediaarchive.dam_bcciucbattingextn ext
               ON ext.id = asset.id
WHERE  asset.isdeleted = 0