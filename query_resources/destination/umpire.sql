SELECT AssetTitle as MAINTITLE,
       S_PARENT_OBJ_ID as ParentObjdId,
       S_UMPIRE_TYPE,
       S_UMPIRE
FROM   bcciuc.mediaarchive.dam_bcciucumpireextn ext
       INNER JOIN bcciuc.mediaarchive.dam_assetbases asset
               ON asset.id = ext.id
WHERE  asset.isdeleted = 0
