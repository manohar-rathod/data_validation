SELECT DISTINCT ( team_id + '_' + Cast(innings AS NVARCHAR(10))
                  + '-' + Cast(bowl_order AS NVARCHAR(50) ) ) AS MAINTITLE,
                --extn.id                                       AS ParentObjdId,
                team_id                                       AS S_TEAM,
                Cast(innings AS NVARCHAR(50))                 AS S_INNINGS,
                Cast(bowl_order AS NVARCHAR(50))              AS S_BOWLINGORDER,
                player                                        AS S_BOWLER,
                Cast(overs AS NVARCHAR(50))                   AS S_OVERS,
                Cast(runs AS NVARCHAR(50))                    AS S_RUNS,
                Cast(wickets AS NVARCHAR(50))                 AS S_WICKETS,
                Cast(maiden AS NVARCHAR(50))                  AS S_MAIDEN,
                Cast(extras AS NVARCHAR(50))                  AS S_EXTRAS,
                Cast(noball AS NVARCHAR(50))                  AS S_NOBALL,
                Cast(wideball AS NVARCHAR(50))                AS S_WIDEBALL,
                economy                                       AS S_ECONOMY,
                match_id                                      AS EX_MATCH_ID
FROM   bcci.clear.pft_bcci_bowling bowling WITH(nolock)
       INNER JOIN bcciuc.mediaarchive.dam_bcciuclevelextn extn WITH(nolock)
               ON extn.ex_match_id = bowling.match_id