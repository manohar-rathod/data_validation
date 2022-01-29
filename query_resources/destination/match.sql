SELECT AssetTitle as matchname,
       ex_match_id,
       s_match_no,
       s_total_matches,
       s_start_date,
       s_end_date,
       s_venue,
       s_stadium,
       s_city,
       s_host,
       s_team_a,
       s_team_b,
       s_match_type,
       ex_iccmatchid,
       s_milestone,
       s_teama_cap,
       s_teama_vc,
       s_teama_wkt_keep,
       s_teamb_cap,
       s_teamb_vc,
       s_teamb_wkt_keep,
       s_commentators,
       s_gender,
       s_sponsor1,
       s_sponsor2,
       s_winning_team,
       s_win_type,
       s_man_of_match,
       s_toss_win,
       s_toss_selection,
       s_result,
       s_last_ball_finish,
       s_last_over_finish,
       s_duck_lew_used,
       ex_dec_effected,
       ex_follow_on,
       s_qualification_stage,
       s_demography,
       s_balls_per_over,
       s_players_by_side,
       s_stadium_in_out,
       s_pitch_type,
       s_desc,
       s_comments,
       s_ballcolor,
       s_bowlindenda,
       s_bowlindendb,
       s_crowdstength,
       s_floodlight,
       s_mandatoryover,
       s_stumpcolor,
       s_tournament,
       s_teama_rep,
       s_teamb_rep,
       s_losing_team,
       s_eventtype,
       s_trophy_name
FROM   bcciuc.mediaarchive.dam_assetbases asset
       INNER JOIN bcciuc.mediaarchive.dam_bcciuclevelextn ext
               ON ext.id = asset.id
WHERE  asset.s_dm_id = 6896
       AND asset.isdeleted = 0