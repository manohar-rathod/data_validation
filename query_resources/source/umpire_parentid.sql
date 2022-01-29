select DISTINCT(ext.id) as id from  mediaarchiveprod.mediaarchive.dmo_bcci_vid vid
       INNER JOIN bcciuc.mediaarchive.dam_bcciuclevelextn ext
               ON ext.ex_match_id COLLATE database_default =
                  vid.a_matchid COLLATE database_default
       INNER JOIN bcciuc.mediaarchive.dam_assetbases asset
               ON asset.id = ext.id