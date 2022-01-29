SELECT DISTINCT
       video.id AS S_PARENT_OBJ_ID
FROM   bcci.clear.pft_bcci_segments_1 seg
       INNER JOIN bcciuc.mediaarchive.dam_bcciucvideosextn video
               ON video.ex_old_dmid = seg.objid
       INNER JOIN bcciuc.mediaarchive.dam_assetbases asset
               ON asset.id = video.id
WHERE  asset.isdeleted = 0