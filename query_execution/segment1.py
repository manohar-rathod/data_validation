import numpy as np
import pandas as pd
import logging

from utils import report
from utils.generic import make_directory, read_sql_file

logger = logging.getLogger(__name__)


class Segment:
    def __init__(self, source_conn, destination_conn):
        self.source_conn=source_conn
        self.destination_conn=destination_conn
        self.source_df=None
        self.destination_df=None

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

            self.title_Capitalize()
            self.snone_empty()
            self.S_TypeOfShot()
            self.S_BallType()
            self.replace_none_values()
            self.convert_none_to_empty()

            src_total_count = src_total_count + len(self.source_df.index)

            # reading and executing sql queries on destination table
            self.destination_df = pd.read_sql(
                f'{read_sql_file("./query_resources/destination/segment1.sql")} and S_PARENT_OBJ_ID = \'{id}\'',
                self.destination_conn)

            #self.dnone_empty()

            dst_total_count = dst_total_count + len(self.destination_df.index)

            #type  conversion
            self.data_type_conversion('S_COMMENTS')
            self.data_type_conversion('S_DAYS')
            self.data_type_conversion('S_SESSION')


            # Sorting all data in source and destination dataframe
            source_sort_df = self.source_df.sort_values(by=self.source_df.columns.tolist()).reset_index(drop=True)
            destination_sort_df = self.destination_df.sort_values(by=self.destination_df.columns.tolist()).reset_index(drop=True)

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


    def snone_empty(self):
        #self.source_df = self.source_df.replace(np.nan, 'blank value', regex=True)
        self.source_df = self.source_df.replace(np.nan, 'None', regex=True)

    #need to remove
    def convert_none_to_empty(self):
        self.source_df = self.source_df.replace('None', '', regex=True)


    def dnone_empty(self):
        self.destination_df = self.destination_df.replace(np.nan, 'blank value', regex=True)

    def S_TypeOfShot(self):
        self.source_df["S_TYPEOFSHOT"] = self.source_df["S_TYPEOFSHOT"].replace("Batsmanstance","Batsman Stance", regex=True)

    def S_BallType(self):
            self.source_df["S_BALLTYPE"] = self.source_df["S_BALLTYPE"].replace("_", " ",regex=True)

   #need transformation
    def S_BoundaryCommentatorsPick(self):
        self.source_df["S_BOUNDARYCOMMENTATORSPICK"] = self.source_df["S_BOUNDARYCOMMENTATORSPICK"].replace("Batsmanstance", "Positive", regex=True)
        self.source_df["S_BOUNDARYCOMMENTATORSPICK"] = self.source_df["S_BOUNDARYCOMMENTATORSPICK"].replace("Batsmanstance", "Negative", regex=True)

    #def S_BoundaryCommentatorsPick(self):


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


        self.column_mapping_replace("S_SESSION", {"SESSION 3": "Session 3", "SESSION 2": "Session 2", "SESSION 1": "Session 1" })
        self.in_for_replace_specific_value('S_SESSION', ['Session 3', 'Session 2', 'Session 1'])

        self.column_mapping_replace("S_WINNINGBOWL",
                                    {"True": "Yes"})
        self.in_for_replace_specific_value('S_WINNINGBOWL', ['Yes'])

    def in_for_replace_specific_value(self, column, values):
        for index in range(len(self.source_df)):
            if self.source_df[column].values[index] not in values:
                self.source_df[column].values[index] = 'None'
            elif column == 'S_WINNINGBOWL' and self.source_df[column].values[index] not in 'Yes':
                self.source_df[column].values[index] = 'No'

            if self.source_df[column].values[index] is None:
                self.source_df[column].values[index] = 'None'

    def column_mapping_replace(self, column, values):
        for key, value in values.items():
            self.source_df[column] = self.source_df[column].replace(key, value, regex=True)

    def data_type_conversion(self, column):
        self.source_df[column] = self.source_df[column].apply(str)
        self.destination_df[column] = self.destination_df[column].apply(str)











