SELECT DISTINCT ( team_id + '' + match_id )   AS MAINTITLE,
                extn.id                       AS ParentObjdId,
                team_id                       AS S_TEAM,
                Cast(innings AS NVARCHAR(20)) AS S_INNINGS,
                Cast(fow AS NVARCHAR(20))     AS S_FOW,
                player_id                     AS S_BATTER,
                Cast(runs AS NVARCHAR(20))    AS S_RUNS,
                overs                         AS S_OVERS,
                match_id                      AS EX_MATCH_ID
FROM   bcci.clear.pft_bcci_fow fow WITH(nolock)
       INNER JOIN bcciuc.mediaarchive.dam_bcciuclevelextn extn WITH(nolock)
               ON extn.ex_match_id = fow.match_id
       INNER JOIN bcciuc.mediaarchive.dam_assetbases asset
               ON asset.id = extn.id
WHERE  asset.isdeleted = 0
       AND asset.s_dm_id = 6896