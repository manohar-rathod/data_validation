import pandas as pd
import logging

from utils import report
from utils.generic import make_directory, read_sql_file

logger = logging.getLogger(__name__)


def umpire_data(source_conn, destination_conn):
    logger.info("umpire table data validation START")

    # to fetch distinct match id
    query = read_sql_file("./query_resources/source/umpire.sql")

    # Extracting all match ids in list
    ids = pd.read_sql(query, source_conn)["PARENTOBJDID"]
    src_total_count, dst_total_count, src_diff_count, dst_diff_count = 0, 0, 0, 0
    # Looping over all match ids to compare
    for id in ids:

        # reading and executing sql queries on source table
        source_df_before_transform = pd.read_sql(
            f'{read_sql_file("./query_resources/source/umpire.sql")} AND  ext.Id = \'{id}\'', source_conn)

        source_df = transform_umpire_data(source_df_before_transform)
        src_total_count = src_total_count + len(source_df.index)

        # reading and executing sql queries on destination table
        destination_df = pd.read_sql(
            f'{read_sql_file("./query_resources/destination/umpire.sql")} and S_PARENT_OBJ_ID = \'{id}\'',
            destination_conn)
        dst_total_count = dst_total_count + len(destination_df.index)
        logger.info(source_df)
        # Sorting all data in source and destination dataframe
        source_sort_df = source_df.sort_values(by=source_df.columns.tolist()).reset_index(drop=True)
        destination_sort_df = destination_df.sort_values(by=destination_df.columns.tolist()).reset_index(drop=True)

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
                make_directory(f'./report/umpire')
                result.to_csv(f'./report/umpire/{id}.csv', index=False)

        except Exception as error:

            # adding csv file for both source and destination table
            make_directory(f'./report/umpire/{id}')
            source_sort_df.to_csv(f'./report/umpire/{id}/source.csv', index=False)
            destination_sort_df.to_csv(f'./report/umpire/{id}/destination.csv', index=False)
            logger.info(id)
            logger.error(f"COUNT MISMATCH original error {error}")

    report.Report.append({"umpire": {"src_total_count": src_total_count, "dst_total_count": dst_total_count,
                                     "diff_count": src_total_count - dst_total_count}})
    logger.info(report.Report)
    logger.info("umpire table data validation END")


def transform_umpire_data(df_data):
    new_data = []

    if df_data["A_FIELDUMPIR"].iloc[0] is not None:
        new_data.append([df_data["MAINTITLE"].iloc[0], df_data["PARENTOBJDID"].iloc[0], 'Field Umpire 1', df_data["A_FIELDUMPIR"].iloc[0]])

    if df_data["A_FIELDUMPI1"].iloc[0] is not None:
        new_data.append([df_data["MAINTITLE"].iloc[0], df_data["PARENTOBJDID"].iloc[0], 'Field Umpire 2', df_data["A_FIELDUMPI1"].iloc[0] ])

    if df_data["A_RES_UMPIRE"].iloc[0] is not None:
        new_data.append([df_data["MAINTITLE"].iloc[0], df_data["PARENTOBJDID"].iloc[0], 'Reserve Umpire', df_data["A_RES_UMPIRE"].iloc[0] ])

    if df_data["A_REFREE"].iloc[0] is not None:
        new_data.append([df_data["MAINTITLE"].iloc[0], df_data["PARENTOBJDID"].iloc[0], 'Match Referee', df_data["A_REFREE"].iloc[0], ])

    if df_data["A_TVUMPIRE"].iloc[0] is not None:
        new_data.append([df_data["MAINTITLE"].iloc[0], df_data["PARENTOBJDID"].iloc[0], 'TV Umpire', df_data["A_TVUMPIRE"].iloc[0], ])

    return pd.DataFrame(new_data, columns=['MAINTITLE', 'PARENTOBJDID', 'S_UMPIRE_TYPE', 'S_UMPIRE'])
