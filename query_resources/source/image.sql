SELECT maintitle                            AS MAINTITLE,
       objid                                AS EX_OLD_DMID,
       a_s_orig_na                          AS EX_OLD_CFN,
       Cast(registration AS NVARCHAR(50))
       + Cast(registratio2 AS NVARCHAR(50)) AS S_CREATED_ON,
       a_caption                            AS EX_CAPTION,
       a_year                               AS S_YEAR,
       a_img_headli                         AS S_HEADLINE,
       a_img_keywor                         AS S_KEYWORD,
       a_s_desc                             AS S_DESC,
       a_orientatio                         AS EX_ORIENTATION,
       a_rights_own                         AS EX_RIGHTS_OWN,
       a_img_headli                         AS EX_IMAGE_HEADLINE,
       a_location                           AS EX_LOCATION
FROM   mediaarchiveprod.mediaarchive.dmo_bcci_ima