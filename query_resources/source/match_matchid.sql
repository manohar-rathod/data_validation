select DISTINCT(Match_Id)
FROM            bcci.clear.pft_bcci_match match
INNER JOIN      mediaarchiveprod.mediaarchive.dmo_bcci_vid vid
ON              match.match_id COLLATE database_default =vid.a_matchid COLLATE database_default
AND             vid.a_s_deleted IS NULL