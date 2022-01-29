SELECT ( 'Segment_' + Isnull(cvs_segmentnum, '0') ) AS maintitle,
       video.id                                     AS s_parent_obj_id,
       starttc                                      AS S_TC_IN,
       endtc                                        AS S_TC_OUT,
       ''                                           AS S_DURATION,
       cvs_segmentnum                               AS EX_Old_Seg_NUM,
       seg.objid                                    AS Ex_Old_Objid,
       cvs_comments                                 AS S_Comments,
       cvs_session                                  AS S_Session,
       cvs_day                                      AS S_Days,
      -- 'None'                                           AS S_CurrentInnings,
       cvs_lineumpire                               AS S_LineUmpire,
       cvs_legumpire                                AS S_LegUmpire,
       cvs_striker                                  AS S_Striker,
       cvs_nonstriker                               AS S_NonStriker,
       cvs_runner_striker                           AS S_Runner,
       cvs_runner_nonstriker                        AS S_NonRunner,
       cvs_bowler                                   AS S_Bowler,
       cvs_winningball                              AS S_WinningBowl,
       cvs_superover                                AS S_SuperOver,
       cvs_overs                                    AS S_Over,
       cvs_teamscore                                AS S_TeamScore,
       cvs_totscore                                 AS S_Score,
       cvs_scorebat                                 AS S_OfftheBat,
       cvs_overthrow                                AS S_Overthrow,
       cvs_shortrun                                 AS S_ShortRun,
       cvs_freehit                                  AS S_FreeHit,
       cvs_extraball_type                           AS S_ExtraBallType,
       cvs_noball_type                              AS S_NoBallType,
       cvs_boundary                                 AS S_BoundaryScored,
       cvs_shottype                                 AS S_TypeOfShot,
       cvs_ball                                     AS S_BallType,
       cvs_commentatorpick                          AS
       S_BoundaryCommentatorsPick,
       cvs_commentatorpick                          AS
       S_BoundaryCommentatorsName,
       cvs_creasepos                                AS S_CreasePosition,
       cvs_thirdumpiredec                           AS S_3rdUmpireDecision,
       cvs_wicket                                   AS S_WicketType,
       cvs_wickettype_qual                          AS S_WICKETTYPE_QUAL,
       ''                                           AS S_BowledType,
       ''                                           AS S_LBWType,
       ''                                           AS S_CatchType,
       cvs_fieldername2                             AS S_Fielder,
       cvs_fieldpos                                 AS S_Location,
       ''                                           AS S_RunOutType,
       ''                                           AS S_StumpedType,
       ''                                           AS S_Player,
       ''                                           AS S_TimedOutPlayer,
       cvs_umpire_refrral                           AS S_3rdUmpireReferralBy,
       cvs_commentatorpick                          AS S_WicketCommentatorsPick,
       cvs_commentatorpick                          AS S_WicketCommentatorsName,
       cvs_angle                                    AS S_BowlingAngle,
       cvs_runup                                    AS S_RunUp,
       cvs_appeal                                   AS S_Appeal,
       cvs_appeal                                   AS S_CatchAppeal,
       cvs_appeal                                   AS S_RunOutAppeal,
       cvs_appeal                                   AS S_LBWAppeal,
       cvs_appeal                                   AS S_StumpingAppeal,
       cvs_appeal                                   AS S_HandlingTheBallAppeal,
       cvs_appeal                                   AS
       S_ObstructingFielderAppeal,
       cvs_appealumpire                             AS S_UmpireAppeal,
       ''                                           AS
       S_3rdUmpireReferralByAppeal,
       cvs_umpirewarning                            AS S_WarningGiven,
       cvs_fieldername                              AS S_FielderName,
       cvs_fieldpos                                 AS S_FielderPosition,
       cvs_fieldingtype                             AS EX_FieldingType,
       cvs_fieldtype                                AS EX_FieldingType1,
       cvs_fieldtype                                AS S_Relay,
       cvs_fieldtype                                AS S_Dive,
       cvs_fieldtype                                AS S_Jump,
       cvs_fieldtype                                AS S_ChaseType,
       cvs_fieldtype                                AS S_PoorFielding,
       cvs_fieldtype                                AS S_BackUp,
       cvs_fieldtype                                AS S_BackUpType,
       ''                                           AS S_PlayerName,
       cvs_commentatorpick                          AS S_CommentatorsPick,
       cvs_commentatorpick                          AS S_CommentatorsName,
       cvs_missedchance                             AS S_MissedChance,
       cvs_missedchancetype                         AS S_MissedChanceType,
       ''                                           AS S_Throw,
       cvs_milestone                                AS S_Milestone,
       cvs_milestonetype                            AS S_MileStoneType,
       cvs_milestoneplayer                          AS S_BattingMilestone,
       cvs_milestone                                AS S_BattingMilestoneDesc,
       cvs_milestone                                AS S_BowlingMilestone,
       cvs_milestone                                AS S_BowlingMilestoneDesc,
       cvs_milestone                                AS S_WicketKeepingMilestone,
       cvs_milestone                                AS
       S_WicketKeepingMilestoneDesc,
       cvs_milestone                                AS S_FieldingMilestone,
       cvs_milestone                                AS S_FieldingMilestoneDesc,
       cvs_milestone                                AS S_AllRoundMilestone,
       cvs_milestone                                AS S_AllRoundMilestoneDesc,
       cvs_milestone                                AS S_PartnershipMilestone,
       cvs_milestone                                AS
       S_PartnershipMilestoneDesc,
       cvs_milestoneplayer                          AS
       S_IndividualMilestonePlayer,
       cvs_milestone                                AS S_IndividualMilestoneDesc
       ,
       cvs_milestone                                AS
       S_UmpiringMilestone,
       cvs_milestone                                AS S_UmpiringMilestoneDesc,
       ''                                           AS S_TeamMilestone,
       ''                                           AS S_HattrickBallMilestone,
       cvs_commentatorpick                          AS
       S_UmpiringCommentatorsPick,
       cvs_commentatorpick                          AS
       S_UmpiringCommentatorsName,
       cvs_commentatorpick                          AS
       S_CaptaincyCommentatorsPick,
       cvs_commentatorpick                          AS
       S_CaptaincyCommentatorsName,
       cvs_commentatorpick                          AS
       S_BowlersRunUpCommentatorsPick,
       cvs_commentatorpick                          AS
       S_BowlersRunUpCommentatorsName,
       cvs_commentatorpick                          AS
       S_RunningbwwicketsCommentatorsPick,
       cvs_commentatorpick                          AS
       S_RunningbwwicketsCommentatorsName,
       cvs_commentatorpick                          AS
       S_WicketKeepingCommentatorsPick,
       cvs_commentatorpick                          AS
       S_WicketKeepingCommentatorsName,
       ''                                           AS S_ExpertViewBattingType,
       cvs_commentatorpick                          AS
       S_BattingTypeCommentatorsPick,
       cvs_commentatorpick                          AS
       S_BattingTypeCommentatorsName,
       ''                                           AS S_ExpertViewBowlingType,
       cvs_commentatorpick                          AS
       S_BowlingTypeCommentatorsPick,
       cvs_commentatorpick                          AS
       S_BowlingTypeCommentatorsName,
       ''                                           AS S_BowlingAction,
       ''                                           AS S_FieldingTypeofSave,
       cvs_commentatorpick                          AS
       S_FieldingCommentatorsPick,
       cvs_commentatorpick                          AS
       S_FieldingCommentatorsName,
       ''                                           AS S_ExpertViewFielder,
       ''                                           AS
       S_ExpertViewFielderPosition,
       ''                                           AS S_Idioms,
       cvs_expert_pos_spirit                        AS
       S_BatsmanWalkingOffBeforeUmpireDecision,
       cvs_expert_pos_spirit                        AS
       S_FieldingCaptainRecallingBatsman,
       cvs_expert_pos_spirit                        AS
       S_FielderApplaudingBatsmen,
       cvs_expert_pos_spirit                        AS
       S_FielderDeclaringBoundaryOnHisOwn,
       cvs_expert_pos_spirit                        AS S_AcceptingADropCatch,
       cvs_expert_neg_spirit                        AS S_UnnecessaryAppeals,
       cvs_expert_neg_spirit                        AS
       S_BatsmanNotAcceptingUmpireDecision,
       cvs_expert_neg_spirit                        AS S_BodyLineBowling,
       cvs_expert_neg_spirit                        AS S_NegativeBowling,
       cvs_expert_neg_spirit                        AS S_BallTampering,
       cvs_expert_neg_spirit                        AS S_PitchTampering,
       cvs_speed                                    AS S_BowlingSpeed,
       cvs_graphics                                 AS S_Graphics,
       ''                                           AS S_GeneralGraphics,
       cvs_playcondition                            AS S_PlayingCondition,
       cvs_analysis_desc                            AS S_GeneralAnalysis,
       ''                                           AS S_SplitScreenBatting,
       ''                                           AS S_SplitScreenBowling,
       ''                                           AS S_SplitScreenFielding,
       cvs_interaction                              AS S_Interaction,
       cvs_emotion                                  AS S_Emotion,
       cvs_emotionplayer                            AS S_EmotionPlayerName,
       cvs_emotion                                  AS S_EmotionalAspects,
       cvs_gesture                                  AS S_Gesture,
       cvs_gestureplayer                            AS S_PlayerShowingGesture,
       cvs_cameraview                               AS S_CameraView,
       cvs_cameraview                               AS S_NormalCrowd,
       cvs_cameraview                               AS S_CheerLeaders,
       cvs_cameraview                               AS S_CatchByCrowd,
       cvs_cameraview                               AS S_Banner,
       cvs_cameraview                               AS S_MexicanWaves,
       cvs_cameraview                               AS S_CommentatorsBox,
       cvs_cameraview                               AS
       S_PavillionDugOutDressingRoom,
       cvs_cameraview                               AS S_ScoreBoard,
       cvs_cameraview                               AS S_ElectronicDisplay,
       cvs_cameraview                               AS S_FlagView,
       cvs_cameraview                               AS S_Ground,
       cvs_cameraview                               AS S_OutsideStadium,
       cvs_cameraview                               AS S_Pitch,
       cvs_cameraview                               AS S_StumpView,
       cvs_cameraview                               AS S_Trophy,
       cvs_cameraview                               AS S_TeamWalkingIntoTheField
       ,
       cvs_cameraview                               AS
       S_PlayerWalkingIntoTheField,
       cvs_cameraview                               AS
       S_UmpiresWalkingIntoTheField,
       cvs_cameraview                               AS S_FieldersClosingIn,
       cvs_camerashot                               AS S_CameraShotType,
       cvs_viewdesc                                 AS S_CelebrityFocus,
       cvs_viewdesc                                 AS S_StyleStatement,
       cvs_extras                                   AS S_Extras,
       cvs_viewdesc                                 AS S_BrandFocus,
       cvs_viewdesc                                 AS S_UsusalCricketGear,
       cvs_viewdesc                                 AS S_PlayerInterview,
       cvs_viewdesc                                 AS S_Memorabilia,
       cvs_viewdesc                                 AS S_PlayerCelebration,
       cvs_interruption                             AS S_Interruption,
       cvs_interruption                             AS S_DueToIncorrectUmpiring,
       cvs_interruption                             AS S_LightsFail,
       cvs_interruption                             AS S_BadWeather,
       cvs_interruption                             AS S_CrowdUnrest,
       cvs_interruption                             AS S_SightScreenAdjustment,
       cvs_interruption                             AS S_StreakerOnField,
       cvs_interruption                             AS S_Bees,
       cvs_interruption                             AS S_Dog,
       cvs_interruption                             AS S_Bird_Birds,
       cvs_interruption                             AS S_BallChange,
       cvs_interruption                             AS S_DrinksBreak,
       cvs_interruption                             AS S_AdHocDrinksBreak,
       cvs_interruption                             AS S_InningsBreak,
       cvs_interruption                             AS S_RollingThePitch,
       cvs_interruption                             AS S_BallLost,
       cvs_interruption                             AS S_ChangeOfGear,
       cvs_injury                                   AS S_Injury,
       cvs_crowdinvasion                            AS S_CrowdInvasion,
       cvs_unusualinfo                              AS S_Unusual,
       cvs_replay                                   AS S_Replay,
       ''                                           AS S_ReplayOver,
       ''                                           AS S_TOTALTEAMWICKET,
       cvs_event                                    AS S_PitchReport,
       cvs_event                                    AS S_Toss,
       ''                                           AS S_TeamARepresentative,
       ''                                           AS S_TeamBRepresentative,
       cvs_event                                    AS S_Interview,
       cvs_event                                    AS S_PracticeSessions,
       cvs_event                                    AS S_Presentation,
       cvs_event                                    AS S_PrizeDistribution,
       cvs_event                                    AS S_VictoryLap,
       ''                                           AS S_AdditionalEvents,
       cvs_event                                    AS S_Events,
       cvs_event                                    AS S_EventType
FROM   bcci.clear.pft_bcci_segments_1 seg
       INNER JOIN bcciuc.mediaarchive.dam_bcciucvideosextn video
               ON video.ex_old_dmid = seg.objid
       INNER JOIN bcciuc.mediaarchive.dam_assetbases asset
               ON asset.id = video.id
WHERE  asset.isdeleted = 0