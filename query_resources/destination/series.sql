SELECT asset.AssetTitle AS MAINTITLE,
       S_SERIES_NAME,
       S_SEASON
FROM   bcciuc.mediaarchive.dam_assetbases asset
       INNER JOIN bcciuc.mediaarchive.dam_bcciuclevelextn ext
               ON ext.id = asset.id
WHERE  asset.s_dm_id = 6895
       AND asset.isdeleted = 0