from datetime import datetime

import numpy as np
import pandas as pd
import logging

from utils import report
from utils.generic import make_directory, read_sql_file

logger = logging.getLogger(__name__)


def match_data(source_conn, destination_conn):
    logger.info("match table data validation START")

    # to fetch distinct match id
    query = read_sql_file("./query_resources/source/match_matchid.sql")
    match_folder_name = f'match_{datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}'
    # Extracting all match ids in list
    match_ids = pd.read_sql(query, source_conn)["MATCH_ID"]
    src_total_count, dst_total_count, src_diff_count, dst_diff_count = 0, 0, 0, 0
    # Looping over all match ids to compare
    for math_id in match_ids:

        # reading and executing sql queries on source table

        make_directory(f'./report/{match_folder_name}/save_data')
        pd.read_sql(
            f'{read_sql_file("./query_resources/source/match.sql")} where Match_Id = \'{math_id}\'', source_conn).to_csv(f'./report/{match_folder_name}/save_data/source_{math_id.replace(" ", "_")}')

        source_df = pd.read_csv(f'./report/{match_folder_name}/save_data/source_{math_id.replace(" ", "_")}').replace(np.nan,'its a blank value',regex=True).drop(['S_RESULT'],axis=1)

        src_total_count = src_total_count + len(source_df.index)

        pd.read_sql(
            f'{read_sql_file("./query_resources/destination/match.sql")} and EX_Match_Id = \'{math_id}\'',
            destination_conn).to_csv(f'./report/{match_folder_name}/save_data/destination_{math_id.replace(" ", "_")}')

        destination_df = pd.read_csv(f'./report/{match_folder_name}/save_data/destination_{math_id.replace(" ", "_")}').replace(
            np.nan, 'its a blank value', regex=True).drop(['S_RESULT'], axis=1)

        dst_total_count = dst_total_count + len(destination_df.index)
        # Sorting all data in source and destination dataframe
        source_sort_df = source_df.sort_values(by=source_df.columns.tolist()).reset_index(drop=True)

        #source_sort_df["S_MILESTONE"] = source_sort_df["S_MILESTONE"].replace('NULL', "NULL", regex=True)
        source_sort_df["S_LAST_BALL_FINISH"] = source_sort_df["S_LAST_BALL_FINISH"].replace("False",0, regex=True).replace("True", 1, regex=True)
        source_sort_df["S_LAST_OVER_FINISH"] = source_sort_df["S_LAST_OVER_FINISH"].replace("False",0, regex=True).replace("True", 1,regex=True)
        source_sort_df["EX_DEC_EFFECTED"] = source_sort_df["EX_DEC_EFFECTED"].replace("False", 0,regex=True).replace("True", 1,regex=True)
        source_sort_df["S_FLOODLIGHT"] = source_sort_df["S_FLOODLIGHT"].replace("False", 0,regex=True).replace("True", 1,regex=True)
        source_sort_df["EX_FOLLOW_ON"] = source_sort_df["EX_FOLLOW_ON"].replace("False", 0, regex=True).replace("True", 1, regex=True)
        destination_sort_df = destination_df.sort_values(by=destination_df.columns.tolist()).reset_index(drop=True)

        if source_sort_df.size == 0:
            make_directory(f'./report/{match_folder_name}/not_in_source')
            destination_sort_df.to_csv(f'./report/{match_folder_name}/not_in_source/{math_id}.csv', index=False)
        else:

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
                    make_directory(f'./report/{match_folder_name}')
                    result.to_csv(f'./report/{match_folder_name}/{math_id}.csv', index=False)


            except Exception as error:

                # to compare match data in destination with source with multiple rows
                check_compare_status = False
                for index, row in source_sort_df.iterrows():
                    for index_1, row_1 in destination_sort_df.iterrows():
                        if row.equals(row_1):
                            check_compare_status = True
                            logger.info("DATA MATCHES WITH SRC AND DST ROWS")
                            break

                        if check_compare_status:
                            break
                if not check_compare_status:
                    # adding csv file for both source and destination table
                    make_directory(f'./report/{match_folder_name}/{math_id.replace(" ", "_")}')
                    source_sort_df.to_csv(f'./report/{match_folder_name}/{math_id.replace(" ", "_")}/source.csv', index=False)
                    destination_sort_df.to_csv(f'./report/{match_folder_name}/{math_id.replace(" ", "_")}/destination.csv',
                                               index=False)
                    logger.error(f"COUNT MISMATCH original error {error}")

    report.Report.append({"match": {"src_total_count": src_total_count, "dst_total_count": dst_total_count,
                                    "diff_count": src_total_count - dst_total_count}})
    logger.info(report.Report)
    logger.info("match table data validation END")
