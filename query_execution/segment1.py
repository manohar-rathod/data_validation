import numpy as np
import pandas as pd
import logging

from query_execution.segment.boolean_value_handle import boolean_value_handle
from query_execution.segment.cameraview import cameraview_cases
from query_execution.segment.milestone import milestone_cases
from utils import report
from utils.generic import make_directory, read_sql_file

logger = logging.getLogger(__name__)


class Segment:
    def __init__(self, source_conn, destination_conn):
        self.source_conn = source_conn
        self.destination_conn = destination_conn
        self.source_df = None
        self.destination_df = None

    def segment1_data(self):
        logger.info("segment1 table data validation START")

        # to fetch distinct match id
        query = read_sql_file("./query_resources/source/segment1_parentid.sql")

        # Extracting all match ids in list
        # ids = pd.read_sql(query, source_conn)["S_PARENT_OBJ_ID"]
        ids = ['1221976']
        src_total_count, dst_total_count, src_diff_count, dst_diff_count = 0, 0, 0, 0
        # Looping over all match ids to compare
        for id in ids:

            # reading and executing sql queries on source table
            self.source_df = pd.read_sql(
                f'{read_sql_file("./query_resources/source/segment1.sql")} and video.id = \'{id}\'', self.source_conn)

            self.source_df.to_csv("./report/save_temp_data/source.csv")



            src_total_count = src_total_count + len(self.source_df.index)

            # reading and executing sql queries on destination table
            self.destination_df = pd.read_sql(
                f'{read_sql_file("./query_resources/destination/segment1.sql")} and S_PARENT_OBJ_ID = \'{id}\'',
                self.destination_conn)

            self.source_df.to_csv("./report/save_temp_data/destination.csv")

            # self.dnone_empty()

            dst_total_count = dst_total_count + len(self.destination_df.index)

            # type  conversion
            self.name_column_data_type_conversion()
            self.data_type_conversion(['S_COMMENTS'])
            self.data_type_conversion(['S_DAYS'])
            self.data_type_conversion(['S_SESSION'])
            self.data_type_conversion(['S_LINEUMPIRE'])
            self.title_Capitalize()
            self.snone_empty()
            self.S_TypeOfShot()
            self.S_BallType()
            self.replace_none_values()



            self.handle_delimiter('S_EMOTIONALASPECTS', ',', 1, 'S_EMOTIONPLAYERNAME')
            self.handle_delimiter('S_EMOTIONPLAYERNAME', ',', 0, 'S_EMOTIONPLAYERNAME')
            self.handle_delimiter('S_GESTURE', ':', 0, 'S_GESTURE')
            self.handle_delimiter('S_GESTURE', ':', 1, 'S_GESTURE')

            self.source_df = self.source_df.replace(r'^\s*$', 'None', regex=True)
            self.replace_nan_with('S_WINNINGBOWL', 'No')
            self.replace_nan_with('S_OFFTHEBAT', '0')
            self.replace_nan_with('S_EXTRAS', '0')
            self.replace_nan_with('S_FREEHIT', 'No')
            self.source_df = boolean_value_handle(self.source_df, self.destination_df).method_replace_yes_no_handle()
            self.source_df = milestone_cases(self.source_df, self.destination_df).milestone()
            self.source_df = cameraview_cases(self.source_df, self.destination_df).camera_view()
            self.destination_df = self.destination_df.replace(r'^\s*$', 'None', regex=True)
            # Sorting all data in source and destination dataframe
            source_sort_df = self.source_df.sort_values(by=self.source_df.columns.tolist()).reset_index(drop=True)
            destination_sort_df = self.destination_df.sort_values(by=self.destination_df.columns.tolist()).reset_index(
                drop=True)

            try:
                '''
                 Comparing two dataframe , if count mismatces (eg: table 1 = 10, table 2= 30)
                 we will create two csv with respective table data inside

                 if count is fine we will check the diff data and 
                 if data diff is not fine will generate 1 csv file with diff data
                '''
                result = source_sort_df.compare(destination_sort_df)

                if result.size != 0:
                    # adding diff data into csv file
                    make_directory(f'./report/segment1')
                    result.to_csv(f'./report/segment1/{id}.csv', index=False)

            except Exception as error:
                print(id)
                # adding csv file for both source and destination table
                make_directory(f'./report/segment1/{id.replace(" ", "_")}')
                source_sort_df.to_csv(f'./report/segment1/{id.replace(" ", "_")}/source.csv', index=False)
                destination_sort_df.to_csv(f'./report/segment1/{id.replace(" ", "_")}/destination.csv', index=False)
                # logger.error(f"COUNT MISMATCH original error {error}")

        report.Report.append({"segment1": {"src_total_count": src_total_count, "dst_total_count": dst_total_count,
                                           "diff_count": src_total_count - dst_total_count}})
        logger.info(report.Report)
        logger.info("segment1 table data validation END")

    def title_Capitalize(self):
        self.source_df['MAINTITLE'] = self.source_df["MAINTITLE"].str.capitalize()
        self.source_df['S_DAYS'] = self.source_df["S_DAYS"].str.capitalize()
        self.source_df['S_CAMERASHOTTYPE'] = self.source_df["S_CAMERASHOTTYPE"].str.capitalize()

    def snone_empty(self):
        # self.source_df = self.source_df.replace(np.nan, 'blank value', regex=True)
        self.source_df = self.source_df.replace(np.nan, 'None', regex=True)

    # need to remove
    def convert_none_to_empty(self):
        self.source_df = self.source_df.replace('None', '', regex=True)

    def dnone_empty(self):
        self.destination_df = self.destination_df.replace(np.nan, 'blank value', regex=True)

    def S_TypeOfShot(self):
        self.source_df["S_TYPEOFSHOT"] = self.source_df["S_TYPEOFSHOT"].replace("Batsmanstance", "Batsman Stance",
                                                                                regex=True)

    def S_BallType(self):
        self.source_df["S_BALLTYPE"] = self.source_df["S_BALLTYPE"].replace("_", " ", regex=True)

    # need transformation
    def S_BoundaryCommentatorsPick(self):
        self.source_df["S_BOUNDARYCOMMENTATORSPICK"] = self.source_df["S_BOUNDARYCOMMENTATORSPICK"].replace(
            "Batsmanstance", "Positive", regex=True)
        self.source_df["S_BOUNDARYCOMMENTATORSPICK"] = self.source_df["S_BOUNDARYCOMMENTATORSPICK"].replace(
            "Batsmanstance", "Negative", regex=True)

    # def S_BoundaryCommentatorsPick(self):

    def replace_none_values(self):
        self.source_df["S_3RDUMPIREREFERRALBY"] = self.source_df["S_3RDUMPIREREFERRALBY"].replace("BATTING_SIDE",
                                                                                                  "Batting Team",
                                                                                                  regex=True)
        self.source_df["S_3RDUMPIREREFERRALBY"] = self.source_df["S_3RDUMPIREREFERRALBY"].replace("BOWLING_SIDE",
                                                                                                  "Bowler", regex=True)
        self.source_df["S_3RDUMPIREREFERRALBY"] = self.source_df["S_3RDUMPIREREFERRALBY"].replace("FIELD_UMPIRE",
                                                                                                  "Field Umpire",
                                                                                                  regex=True)
        self.in_for_replace_specific_value('S_3RDUMPIREREFERRALBY', ['Batting Team', 'Bowler', 'Field Umpir'])

        self.source_df["S_CREASEPOSITION"] = self.source_df["S_CREASEPOSITION"].replace("DOWN_THE_CREASE",
                                                                                        "Down The Pitch", regex=True)
        self.source_df["S_CREASEPOSITION"] = self.source_df["S_CREASEPOSITION"].replace("ON_THE_CREASE",
                                                                                        "On The Crease", regex=True)

        self.in_for_replace_specific_value('S_CREASEPOSITION', ['Down The Pitch', 'On The Crease'])

        self.source_df["S_NOBALLTYPE"] = self.source_df["S_NOBALLTYPE"].replace("ABOVESHOULDER", "Above Shoulder",
                                                                                regex=True)
        self.source_df["S_NOBALLTYPE"] = self.source_df["S_NOBALLTYPE"].replace("OVERSTEPPING", "Over Stepping",
                                                                                regex=True)
        self.in_for_replace_specific_value('S_NOBALLTYPE', ['Above Shoulder', 'Over Stepping'])

        self.source_df["S_EXTRABALLTYPE"] = self.source_df["S_EXTRABALLTYPE"].replace("BALLHITTINGHELMET",
                                                                                      "BallHittingHelmet", regex=True)
        self.source_df["S_EXTRABALLTYPE"] = self.source_df["S_EXTRABALLTYPE"].replace("BYE", "Byes", regex=True)
        self.source_df["S_EXTRABALLTYPE"] = self.source_df["S_EXTRABALLTYPE"].replace("LEGBYE", "LegByes", regex=True)
        self.source_df["S_EXTRABALLTYPE"] = self.source_df["S_EXTRABALLTYPE"].replace("WIDE", "Wide", regex=True)
        self.source_df["S_EXTRABALLTYPE"] = self.source_df["S_EXTRABALLTYPE"].replace("NO", "NoBall", regex=True)
        self.in_for_replace_specific_value('S_EXTRABALLTYPE', ['Byes', 'LegByes', 'Wide', 'NoBall'])

        self.column_mapping_replace("S_SESSION",
                                    {"SESSION 3": "Session 3", "SESSION 2": "Session 2", "SESSION 1": "Session 1"})
        self.in_for_replace_specific_value('S_SESSION', ['Session 3', 'Session 2', 'Session 1'])

        self.column_mapping_replace("S_WINNINGBOWL",
                                    {"True": "Yes"})
        self.in_for_replace_specific_value('S_WINNINGBOWL', ['Yes'])

        self.column_mapping_replace("S_BOWLINGANGLE",
                                    {"ACROSS_THE_WICKET": "Across The Wicke", "AROUND_THE_WICKET": "Round The Wicket",
                                     "OVER_THE_WICKET": "Over The Wicket"})
        self.in_for_replace_specific_value('S_BOWLINGANGLE',
                                           ['Across The Wicke', 'Round The Wicket', 'Over The Wicket'])
        self.in_for_replace_specific_value('S_CATCHAPPEAL',
                                           ['Normal', 'Bat pad', 'Glove'])

        self.in_for_replace_specific_value('S_RUNOUTAPPEAL',
                                           ['By Deflection', 'Regular'])

        self.in_for_replace_specific_value('S_RUNOUTAPPEAL',
                                           ['By Deflection', 'Regular'])

        self.column_mapping_replace("S_MISSEDCHANCE",
                                    {"CATCH": "Catch", "RUNOUT": "Run Out",
                                     "STUMPED": "Stumping"})
        self.in_for_replace_specific_value('S_MISSEDCHANCE',
                                           ['Catch', 'Run Out', 'Stumping'])

        self.column_mapping_replace("S_GRAPHICS",
                                    {"BOWLINGREGIONMAP": "Bowling Regionmap",
                                     "DISTANCEHIT": "Distance Hit",
                                     "GENERALGRAPHICS": "General Graphics",
                                     "HAWKEYE": "Hawk Eye",
                                     "PLAYERSTATISTICS": "Player Statistics",
                                     "SNICKOMETER": "Snickometer",
                                     "SPEEDOFFBAT": "Speed Off Bat",
                                     "STUMPVISION": "Stump Vision",
                                     "TEAMSTATISTICS": "Team Statistics",
                                     "WAGONWHEEL": "Wagon Wheel",
                                     })
        self.in_for_replace_specific_value('S_GRAPHICS',
                                           ['Bowling Regionmap', 'Distance Hit', 'General Graphics', 'Hawk Eye',
                                            'Player Statistics', 'Snickometer', 'Speed Off Bat', 'Stump Vision',
                                            'Team Statistics', 'Wagon Wheel'])

        self.column_mapping_replace("S_PLAYINGCONDITION",
                                    {"DIMLIGHT": "Dim Light", "DRIZZLE": "Drizzle",
                                     "HOTHUMID": "Hot And Humid", "Normal": "Normal", "WETOUTFIELD": "Wet Outfield"})
        self.in_for_replace_specific_value('S_PLAYINGCONDITION',
                                           ['Dim Light', 'Drizzle', 'Hot And Humid', 'Normal', 'Wet Outfield'])

        self.column_mapping_replace("S_INTERACTION",
                                    {"CONFRONTATION": "Confrontation", "CONSULTATION": "Consultation",
                                     "SLEDGING": "Sledging"})
        self.in_for_replace_specific_value('S_INTERACTION',
                                           ['Confrontation', 'Consultation', 'Sledging'])

        self.column_mapping_replace("S_INJURY",
                                    {"BATSMAN": "Batsman", "BOWLER": "Bowler",
                                     "FIELDER": "Fielder", "UMPIRE": "Umpire"})
        self.in_for_replace_specific_value('S_INJURY',
                                           ['Batsman', 'Bowler', 'Fielder', 'Umpire'])

        self.column_mapping_replace("S_CROWDINVASION",
                                    {"DURINGTHEMATCH": "During The Match", "AFTERVICTORY": "After Victory",
                                     "BEFORETHEMATCH": "Before The Match"})
        self.in_for_replace_specific_value('S_CROWDINVASION',
                                           ['During The Match', 'After Victory', 'Before The Match'])

    def in_for_replace_specific_value(self, column, values):
        for index in range(len(self.source_df)):
            if self.source_df[column].values[index] not in values:
                self.source_df[column].values[index] = 'None'

    def column_mapping_replace(self, column, values):
        for key, value in values.items():
            self.source_df[column] = self.source_df[column].replace(key, value, regex=True)

    def data_type_conversion(self, columns):
        for column in columns:
            self.source_df[column] = self.source_df[column].apply(str)
            self.destination_df[column] = self.destination_df[column].apply(str)

    def name_column_data_type_conversion(self):
        self.data_type_conversion(['S_BOUNDARYCOMMENTATORSNAME',
                                   'S_WICKETCOMMENTATORSNAME',
                                   'S_COMMENTATORSNAME',
                                   'S_UMPIRINGCOMMENTATORSNAME',
                                   'S_CAPTAINCYCOMMENTATORSNAME',
                                   'S_BOWLERSRUNUPCOMMENTATORSNAME',
                                   'S_RUNNINGBWWICKETSCOMMENTATORSNAME',
                                   'S_WICKETKEEPINGCOMMENTATORSNAME',
                                   'S_BATTINGTYPECOMMENTATORSNAME',
                                   'S_BOWLINGTYPECOMMENTATORSNAME',
                                   'S_FIELDINGCOMMENTATORSNAME',
                                   'S_UMPIRINGCOMMENTATORSNAME', 'S_CAPTAINCYCOMMENTATORSNAME',
                                   'S_BOWLERSRUNUPCOMMENTATORSNAME',
                                   'S_RUNNINGBWWICKETSCOMMENTATORSNAME',
                                   'S_WICKETKEEPINGCOMMENTATORSNAME',
                                   'S_BATTINGTYPECOMMENTATORSNAME',
                                   'S_BOWLINGTYPECOMMENTATORSNAME',
                                   'S_FIELDINGCOMMENTATORSNAME',
                                   'S_CAPTAINCYCOMMENTATORSNAME',
                                   'S_BOWLERSRUNUPCOMMENTATORSNAME',
                                   'S_RUNNINGBWWICKETSCOMMENTATORSNAME',
                                   'S_WICKETKEEPINGCOMMENTATORSNAME',
                                   'S_BATTINGTYPECOMMENTATORSNAME',
                                   'S_BATTINGTYPECOMMENTATORSNAME',
                                   'S_FIELDINGCOMMENTATORSNAME'
                                   ])

    def replace_nan_with(self, column, value_to_replace):
        self.source_df[column] = self.source_df[column].replace('None', value_to_replace, regex=True)

    def make_changes_on_csv(self):
        self.source_df = self.source_df.replace(r'^\s*$', 'None', regex=True)
        self.destination_df = self.destination_df.replace(r'^\s*$', 'None', regex=True)

    def handle_delimiter(self, column, delimeter, part, column_to_look):
        for index in range(len(self.source_df)):
            try:
                self.source_df[column].values[index] = self.source_df[column_to_look].values[index].split(',')[part]
            except Exception as error:
                logger.info(error)
            
            try:
               self.source_df[column].values[index] = self.source_df[column_to_look].values[index].split(":")[part]
             except Exception as error:
                 logger.info(error)
