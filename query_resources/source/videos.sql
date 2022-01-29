SELECT vid.maintitle                                     AS maintitle,
       vid.objid                                         AS ex_old_dmid,
       vid.a_s_orig_na                                   AS ex_old_cfn,
       Cast(match.match_id AS NVARCHAR(max))             AS ex_match_id,
       Substring(vid.registration + registratio2, 1, 4)
       + '-'
       + Substring(vid.registration + registratio2, 5, 2)
       + '-'
       + Substring(vid.registration + registratio2, 7, 2)
       + 'T'
       + Substring(vid.registration + registratio2, 9, 2)
       + ':'
       + Substring(vid.registration + registratio2, 11, 2)
       + ':'
       + Substring(vid.registration + registratio2, 13, 3)
       + '.000'                                          AS s_created_on,
       vid.a_session                                     AS s_session,
       vid.a_day                                         AS s_day_num,
       vid.a_innings                                     AS s_innings,
       vid.a_s_desc                                      AS s_desc,
       vid.a_comments                                    AS s_comments
FROM   mediaarchiveprod.mediaarchive.dmo_bcci_vid vid
       LEFT JOIN bcciuc.clear.pft_bcci_match match
              ON match.match_id COLLATE database_default =
                 vid.a_matchid COLLATE database_default
WHERE  vid.a_s_deleted IS NULL
       AND match.match_id IS NOT NULL