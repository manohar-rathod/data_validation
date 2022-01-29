SELECT assettitle AS MAINTITLE,
       s_parent_obj_id,
       asset.s_tc_in,
       asset.s_tc_out,
       s_duration,
       ex_old_seg_num,
       ex_old_objid,
       s_comments,
       s_session,
       s_days,
      -- s_currentinnings,
       s_lineumpire,
       s_legumpire,
       s_striker,
       s_nonstriker,
       s_runner,
       s_nonrunner,
       s_bowler,
       s_winningbowl,
       s_superover,
       s_over,
       s_teamscore,
       s_score,
       s_offthebat,
       s_overthrow,
       s_shortrun,
       s_freehit,
       s_extraballtype,
       s_noballtype,
       s_boundaryscored,
       s_typeofshot,
       s_balltype,
       s_boundarycommentatorspick,
       s_boundarycommentatorsname,
       s_creaseposition,
       s_3rdumpiredecision,
       s_wickettype,
       s_wickettype_qual,
       s_bowledtype,
       s_lbwtype,
       s_catchtype,
       s_fielder,
       s_location,
       s_runouttype,
       s_stumpedtype,
       s_player,
       s_timedoutplayer,
       s_3rdumpirereferralby,
       s_wicketcommentatorspick,
       s_wicketcommentatorsname,
       s_bowlingangle,
       s_runup,
       s_appeal,
       s_catchappeal,
       s_runoutappeal,
       s_lbwappeal,
       s_stumpingappeal,
       s_handlingtheballappeal,
       s_obstructingfielderappeal,
       s_umpireappeal,
       s_3rdumpirereferralbyappeal,
       s_warninggiven,
       s_fieldername,
       s_fielderposition,
       ex_fieldingtype,
       ex_fieldingtype1,
       s_relay,
       s_dive,
       s_jump,
       s_chasetype,
       s_poorfielding,
       s_backup,
       s_backuptype,
       s_playername,
       s_commentatorspick,
       s_commentatorsname,
       s_missedchance,
       s_missedchancetype,
       s_throw,
       s_milestone,
       s_milestonetype,
       s_battingmilestone,
       s_battingmilestonedesc,
       s_bowlingmilestone,
       s_bowlingmilestonedesc,
       s_wicketkeepingmilestone,
       s_wicketkeepingmilestonedesc,
       s_fieldingmilestone,
       s_fieldingmilestonedesc,
       s_allroundmilestone,
       s_allroundmilestonedesc,
       s_partnershipmilestone,
       s_partnershipmilestonedesc,
       s_individualmilestoneplayer,
       s_individualmilestonedesc,
       s_umpiringmilestone,
       s_umpiringmilestonedesc,
       s_teammilestone,
       s_hattrickballmilestone,
       s_umpiringcommentatorspick,
       s_umpiringcommentatorsname,
       s_captaincycommentatorspick,
       s_captaincycommentatorsname,
       s_bowlersrunupcommentatorspick,
       s_bowlersrunupcommentatorsname,
       s_runningbwwicketscommentatorspick,
       s_runningbwwicketscommentatorsname,
       s_wicketkeepingcommentatorspick,
       s_wicketkeepingcommentatorsname,
       s_expertviewbattingtype,
       s_battingtypecommentatorspick,
       s_battingtypecommentatorsname,
       s_expertviewbowlingtype,
       s_bowlingtypecommentatorspick,
       s_bowlingtypecommentatorsname,
       s_bowlingaction,
       s_fieldingtypeofsave,
       s_fieldingcommentatorspick,
       s_fieldingcommentatorsname,
       s_expertviewfielder,
       s_expertviewfielderposition,
       s_idioms,
       s_batsmanwalkingoffbeforeumpiredecision,
       s_fieldingcaptainrecallingbatsman,
       s_fielderapplaudingbatsmen,
       s_fielderdeclaringboundaryonhisown,
       s_acceptingadropcatch,
       s_unnecessaryappeals,
       s_batsmannotacceptingumpiredecision,
       s_bodylinebowling,
       s_negativebowling,
       s_balltampering,
       s_pitchtampering,
       s_bowlingspeed,
       s_graphics,
       s_generalgraphics,
       s_playingcondition,
       s_generalanalysis,
       s_splitscreenbatting,
       s_splitscreenbowling,
       s_splitscreenfielding,
       s_interaction,
       s_emotion,
       s_emotionplayername,
       s_emotionalaspects,
       s_gesture,
       s_playershowinggesture,
       s_cameraview,
       s_normalcrowd,
       s_cheerleaders,
       s_catchbycrowd,
       s_banner,
       s_mexicanwaves,
       s_commentatorsbox,
       s_pavilliondugoutdressingroom,
       s_scoreboard,
       s_electronicdisplay,
       s_flagview,
       s_ground,
       s_outsidestadium,
       s_pitch,
       s_stumpview,
       s_trophy,
       s_teamwalkingintothefield,
       s_playerwalkingintothefield,
       s_umpireswalkingintothefield,
       s_fieldersclosingin,
       s_camerashottype,
       s_celebrityfocus,
       s_stylestatement,
       s_extras,
       s_brandfocus,
       s_ususalcricketgear,
       s_playerinterview,
       s_memorabilia,
       s_playercelebration,
       s_interruption,
       s_duetoincorrectumpiring,
       s_lightsfail,
       s_badweather,
       s_crowdunrest,
       s_sightscreenadjustment,
       s_streakeronfield,
       s_bees,
       s_dog,
       s_bird_birds,
       s_ballchange,
       s_drinksbreak,
       s_adhocdrinksbreak,
       s_inningsbreak,
       s_rollingthepitch,
       s_balllost,
       s_changeofgear,
       s_injury,
       s_crowdinvasion,
       s_unusual,
       s_replay,
       s_replayover,
       s_totalteamwicket,
       s_pitchreport,
       s_toss,
       s_teamarepresentative,
       s_teambrepresentative,
       s_interview,
       s_practicesessions,
       s_presentation,
       s_prizedistribution,
       s_victorylap,
       s_additionalevents,
       s_events,
       s_eventtype
FROM   bcciuc.mediaarchive.dam_bcciucsegmentextn ext
       INNER JOIN bcciuc.mediaarchive.dam_assetbases asset
               ON asset.id = ext.id
WHERE  asset.isdeleted = 0