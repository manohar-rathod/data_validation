SELECT DISTINCT(Ltrim(Rtrim(Cast(team_a AS NVARCHAR(max))))+' - '+ ltrim(rtrim(cast(team_b AS nvarchar(max))))+' - '+replace(isnull(starting_date,''),'.','-'))            AS matchname,
                cast(match_id AS               nvarchar(max))                                                                                                              AS ex_match_id,
                cast(a_matchno AS              nvarchar(max))                                                                                                              AS s_match_no,
                cast(a_totalmatch AS           nvarchar(max))                                                                                                              AS s_total_matches,
                cast(starting_date AS          nvarchar(max))                                                                                                              AS s_start_date,
                cast(ending_date AS            nvarchar(max))                                                                                                              AS s_end_date,
                cast(venue AS                  nvarchar(max))                                                                                                              AS s_venue,
                cast(a_stadium AS              nvarchar(max))                                                                                                              AS s_stadium,
                cast(a_city AS                 nvarchar(max))                                                                                                              AS s_city,
                cast(a_host AS                 nvarchar(max))                                                                                                              AS s_host,
                cast(a_teama AS                nvarchar(max))                                                                                                              AS s_team_a,
                cast(a_teamb AS                nvarchar(max))                                                                                                              AS s_team_b,
                cast(a_matchtype AS            nvarchar(max))                                                                                                              AS s_match_type,
                cast(icc_match_id AS           nvarchar(max))                                                                                                              AS ex_iccmatchid,
                cast(a_milestone AS            nvarchar(max))                                                                                                              AS s_milestone,
                cast(a_teama_capt AS           nvarchar(max))                                                                                                              AS s_teama_cap,
                cast(a_teama_vice AS           nvarchar(max))                                                                                                              AS s_teama_vc,
                cast(a_teama_wktk AS           nvarchar(max))                                                                                                              AS s_teama_wkt_keep,
                cast(a_teamb_capt AS           nvarchar(max))                                                                                                              AS s_teamb_cap,
                cast(a_teamb_vice AS           nvarchar(max))                                                                                                              AS s_teamb_vc,
                cast(a_teamb_wktk AS           nvarchar(max))                                                                                                              AS s_teamb_wkt_keep,
                cast(a_commentato AS           nvarchar(max))                                                                                                              AS s_commentators,
                cast(a_gender AS               nvarchar(max))                                                                                                              AS s_gender,
                cast(a_sponsor_ot AS           nvarchar(max))                                                                                                              AS s_sponsor1,
                cast(a_sponsor AS              nvarchar(max))                                                                                                              AS s_sponsor2,
                cast(a_win_team AS             nvarchar(max))                                                                                                              AS s_winning_team,
                cast(a_win_type AS             nvarchar(max))                                                                                                              AS s_win_type,
                cast(a_mom AS                  nvarchar(max))                                                                                                              AS s_man_of_match,
                cast(a_toss_win AS             nvarchar(max))                                                                                                              AS s_toss_win,
                cast(a_toss_selec AS           nvarchar(max))                                                                                                              AS s_toss_selection,
                cast(a_match_resu AS           nvarchar(max))                                                                                                              AS s_result,
                cast(a_lastballfi AS           nvarchar(max))                                                                                                              AS s_last_ball_finish,
                cast(a_lastoverfi AS           nvarchar(max))                                                                                                              AS s_last_over_finish,
                cast(duckworth_lewis_used AS   nvarchar(max))                                                                                                              AS s_duck_lew_used,
                cast(declaration_effected AS   nvarchar(max))                                                                                                              AS ex_dec_effected,
                cast(followon_effected_by AS   nvarchar(max))                                                                                                              AS ex_follow_on,
                cast(a_qual_stage AS           nvarchar(max))                                                                                                              AS s_qualification_stage,
                cast(a_demography AS           nvarchar(max))                                                                                                              AS s_demography,
                cast(balls_per_over AS         nvarchar(max))                                                                                                              AS s_balls_per_over,
                cast(players_by_side AS        nvarchar(max))                                                                                                              AS s_players_by_side,
                cast(staduim_indoor_outdoor AS nvarchar(max))                                                                                                              AS s_stadium_in_out,
                cast(pitch_type AS             nvarchar(max))                                                                                                              AS s_pitch_type,
                cast(a_s_desc AS               nvarchar(max))                                                                                                              AS s_desc,
                cast(a_comments AS             nvarchar(max))                                                                                                              AS s_comments,
                cast(a_ballcolor AS            nvarchar(max))                                                                                                              AS s_ballcolor,
                cast(a_bowl_enda AS            nvarchar(max))                                                                                                              AS s_bowlindenda,
                cast(a_bowl_endb AS            nvarchar(max))                                                                                                              AS s_bowlindendb,
                cast(a_crowdstatu AS           nvarchar(max))                                                                                                              AS s_crowdstength,
                cast(a_floodlight AS           nvarchar(max))                                                                                                              AS s_floodlight,
                cast(a_mandatoryo AS           nvarchar(max))                                                                                                              AS s_mandatoryover,
                cast(a_stumpcolor AS           nvarchar(max))                                                                                                              AS s_stumpcolor,
                cast(a_tournament AS           nvarchar(max))                                                                                                              AS s_tournament,
                cast(a_teama_rep AS            nvarchar(max))                                                                                                              AS s_teama_rep,
                cast(a_teamb_rep AS            nvarchar(max))                                                                                                              AS s_teamb_rep,
                cast(a_losing_tea AS           nvarchar(max))                                                                                                              AS s_losing_team,
                cast(a_event_type AS           nvarchar(max))                                                                                                              AS s_eventtype,
                cast(trophy_name AS            nvarchar(max))                                                                                                              AS s_trophy_name
FROM            BCCI.clear.pft_bcci_match match
INNER JOIN      mediaarchiveprod.mediaarchive.dmo_bcci_vid vid
ON              match.match_id COLLATE database_default =vid.a_matchid COLLATE database_default
AND             vid.a_s_deleted IS NULL
AND             a_qual_stage IS NOT NULL