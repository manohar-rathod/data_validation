SELECT DISTINCT match.series_name+' '+REPLACE (match.year,'/','_') AS MAINTITLE,
                match.series_name                                                               AS S_SERIES_NAME,
                match.year                                                                      AS S_SEASON,
                cast(match.match_category AS nvarchar(max))                                     AS matchcategory
FROM            mediaarchiveprod.mediaarchive.dmo_bcci_vid vid
INNER JOIN      bcci.clear.pft_bcci_match match
ON              match.match_id COLLATE database_default =vid.a_matchid COLLATE database_default
WHERE           vid.a_s_deleted IS NULL