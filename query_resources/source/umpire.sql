SELECT DISTINCT ( Cast(a_teama AS NVARCHAR(max)) + '-'
                  + Cast(a_teamb AS NVARCHAR(max)) ) AS MAINTITLE,
                ext.id                               AS ParentObjdId,
                Cast(a_fieldumpir AS NVARCHAR(1000)) AS A_FIELDUMPIR,
                Cast(a_fieldumpi1 AS NVARCHAR(1000)) AS A_FIELDUMPI1,
                Cast(a_res_umpire AS NVARCHAR(1000)) AS A_RES_UMPIRE,
                Cast(a_refree AS NVARCHAR(1000))     AS A_REFREE,
                Cast(a_tvumpire AS NVARCHAR(1000))   AS A_TVUMPIRE
FROM   mediaarchiveprod.mediaarchive.dmo_bcci_vid vid
       INNER JOIN bcciuc.mediaarchive.dam_bcciuclevelextn ext
               ON ext.ex_match_id COLLATE database_default =
                  vid.a_matchid COLLATE database_default
       INNER JOIN bcciuc.mediaarchive.dam_assetbases asset
               ON asset.id = ext.id
WHERE  asset.isdeleted = 0
       AND a_matchid <> ''
       AND asset.s_dm_id = 6896
       AND a_teama IS NOT NULL
       AND a_teamb IS NOT NULL