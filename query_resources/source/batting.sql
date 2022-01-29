SELECT  DISTINCT ( team_id + '_'
                  + Cast(bat_order AS NVARCHAR(50)) ) AS MAINTITLE,
               -- extn.id                               AS ParentObjId,
                team_id                               AS S_TEAM,
                Cast(bat_order AS NVARCHAR(50))       AS S_BAT_ORDER,
                player_id                             AS S_BATTER,
                wkt_type                              AS S_WICKETTYPE,
                fielder                               AS S_FIELDERNAME,
                bowler                                AS S_BOWLER,
                Cast(runs AS NVARCHAR(50))            AS S_RUNS,
                Cast(minutes AS NVARCHAR(50))         AS S_MINUTES,
                Cast(fours AS NVARCHAR(50))           AS S_FOURS,
                Cast(six AS NVARCHAR(50))             AS S_SIXES,
                strikerate                            AS S_STRIKERATE,
                Cast(balls AS NVARCHAR(50))           AS S_BALLS,
                Cast(innings AS NVARCHAR(50))         AS S_INNINGS,
                match_id                              AS EX_MATCH_ID
FROM   bcci.clear.pft_bcci_batting batting
       INNER JOIN bcciuc.mediaarchive.dam_bcciuclevelextn extn
               ON extn.ex_match_id = batting.match_id