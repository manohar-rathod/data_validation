SELECT        ( team_id + '_' + match_id )  AS MAINTITLE,
              extn.folderid                 AS ParentFolderId,
              team_id                       AS S_TEAM,
              Cast(innings AS NVARCHAR(50)) AS S_INNINGS,
              match_id                      AS EX_MATCH_ID,
              Cast(pp_no AS NVARCHAR(50))   AS S_PP_NO,
              overs                         AS S_OVERS,
              ref_by                        AS S_REF_BY
FROM   bcci.clear.pft_bcci_powerplay pp
       INNER JOIN bcciuc.mediaarchive.dam_bcciuclevelextn extn
               ON extn.ex_match_id = pp.match_id