from datetime import datetime

import numpy as np
import pandas as pd
import logging

from utils import report
from utils.generic import make_directory, read_sql_file

logger = logging.getLogger(__name__)


def videos_data(source_conn, destination_conn):
    logger.info("videos table data validation START")

    # to fetch distinct match id
    query = 'select DISTINCT(Match_Id) from BCCI.clear.pft_bcci_match where Match_Id != \' \''

    # Extracting all match ids in list
    match_ids = pd.read_sql(query, source_conn)["Match_Id"]
    src_total_count, dst_total_count, src_diff_count, dst_diff_count = 0, 0, 0, 0

    video_folder_name = f'videos_{datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}'

    # Looping over all match ids to compare
    for math_id in match_ids:
        # reading and executing sql queries on source table
        source_df = pd.read_sql(
            f'{read_sql_file("./query_resources/source/videos.sql")} and Match_Id = \'{math_id}\'',
            source_conn)
        source_df["EX_OLD_DMID"] = source_df["EX_OLD_DMID"].apply(str)
        source_df["S_CREATED_ON"] = source_df["S_CREATED_ON"].apply(str)
        src_total_count = src_total_count + len(source_df.index)

        # reading and executing sql queries on destination table
        destination_df = pd.read_sql(
            f'{read_sql_file("./query_resources/destination/videos.sql")} and EX_Match_Id = \'{math_id}\'',
            destination_conn)

        destination_df["EX_OLD_DMID"] = destination_df["EX_OLD_DMID"].apply(str)
        destination_df["S_CREATED_ON"] = destination_df["S_CREATED_ON"].apply(str)
        dst_total_count = dst_total_count + len(destination_df.index)

        # Sorting all data in source and destination dataframe
        source_sort_df = source_df.sort_values(by=source_df.columns.tolist()).reset_index(drop=True).replace(np.nan, 'blank value', regex=True)
        destination_sort_df = destination_df.sort_values(by=destination_df.columns.tolist()).reset_index(drop=True)
        make_directory(f'./report/{video_folder_name}/save_video_data')
        destination_sort_df.to_csv(f'./report/{video_folder_name}/save_video_data/{math_id.replace(" ", "_")}.csv',
                                   index=False)
        destination_sort_df = pd.read_csv(f'./report/{video_folder_name}/save_video_data/{math_id.replace(" ", "_")}.csv').replace(np.nan, 'blank value', regex=True)

        destination_sort_df["EX_OLD_DMID"] = destination_sort_df["EX_OLD_DMID"].apply(str)
        destination_sort_df["EX_MATCH_ID"] = destination_sort_df["EX_MATCH_ID"].apply(str)

        destination_sort_df["S_SESSION"] = destination_sort_df["S_SESSION"].apply(str)
        source_sort_df["S_SESSION"] = destination_sort_df["S_SESSION"].apply(str)


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
                make_directory(f'./report/{video_folder_name}')
                result.to_csv(f'./report/{video_folder_name}/{math_id}.csv', index=False)
                logger.info("DATA MISMATCH")
        except Exception as error:
            # adding csv file for both source and destination table
            make_directory(f'./report/{video_folder_name}/{math_id.replace(" ", "_")}')
            source_sort_df.to_csv(f'./report/{video_folder_name}/{math_id.replace(" ", "_")}/source.csv', index=False)
            destination_sort_df.to_csv(f'./report/{video_folder_name}/{math_id.replace(" ", "_")}/destination.csv', index=False)
            logger.error(f"COUNT MISMATCH original error {error}")

    report.Report.append({"videos": {"src_total_count": src_total_count, "dst_total_count": dst_total_count,
                                     "diff_count": src_total_count - dst_total_count}})
    logger.info(report.Report)
    logger.info("videos table data validation END")
