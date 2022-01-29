SELECT AssetTitle AS MAINTITLE,
       EX_OLD_DMID,
       EX_OLD_CFN,
       S_CREATED_ON,
       EX_CAPTION,
       S_YEAR,
       S_HEADLINE,
       S_KEYWORD,
       S_DESC,
       EX_ORIENTATION,
       EX_RIGHTS_OWN,
       EX_IMAGE_HEADLINE,
       EX_LOCATION
FROM   bcciuc.mediaarchive.dam_assetbases asset
       INNER JOIN bcciuc.mediaarchive.dam_bcciucimagesextn ext
               ON ext.id = asset.id
WHERE  asset.isdeleted = 0 