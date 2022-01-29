SELECT asset.assettitle      AS MAINTITLE,
       asset.S_PARENT_FOLDER_ID AS ParentFolderId,
       S_TEAM,
       S_INNINGS,
       EX_MATCH_ID,
       S_PP_NO,
       S_OVERS,
       S_REF_BY
FROM   bcciuc.mediaarchive.dam_assetbases asset
       INNER JOIN bcciuc.mediaarchive.dam_bcciucpowerplayextn ext
               ON ext.id = asset.id
WHERE  asset.isdeleted = 0