import pandas as pd
import logging

from utils import report
from utils.generic import make_directory, read_sql_file

logger = logging.getLogger(__name__)


def image_data(source_conn, destination_conn):
    logger.info("image table data validation START")

    # to fetch distinct match id
    query = 'SELECT DISTINCT objid FROM   mediaarchiveprod.mediaarchive.dmo_bcci_ima'

    # Extracting all match ids in list
    objids = pd.read_sql(query, source_conn)["objid"]
    src_total_count, dst_total_count, src_diff_count, dst_diff_count = 0, 0, 0, 0
    # Looping over all match ids to compare

    for index, objid in enumerate(make_batch(objids, 10)):

        # reading and executing sql queries on source table
        source_df = pd.read_sql(
            f'{read_sql_file("./query_resources/source/image.sql")} where objid in {objid}', source_conn)
        src_total_count = src_total_count + len(source_df.index)

        # reading and executing sql queries on destination table
        destination_df = pd.read_sql(
            f'{read_sql_file("./query_resources/destination/image.sql")} and EX_OLD_DMID in {objid}',
            destination_conn)
        dst_total_count = dst_total_count + len(destination_df.index)

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
                make_directory(f'./report/image')
                result.to_csv(f'./report/image/{index}.csv', index=False)

        except Exception as error:

            # adding csv file for both source and destination table
            make_directory(f'./report/image/{index.replace(" ", "_")}')
            source_sort_df.to_csv(f'./report/image/{index.replace(" ", "_")}/source.csv', index=False)
            destination_sort_df.to_csv(f'./report/image/{index.replace(" ", "_")}/destination.csv', index=False)
            logger.error(f"COUNT MISMATCH original error {error}")

    report.Report.append({"image": {"src_total_count": src_total_count, "dst_total_count": dst_total_count,
                                    "diff_count": src_total_count - dst_total_count}})
    logger.info(report.Report)
    logger.info("image table data validation END")


def make_batch(data, number):
    appendable_data = ""
    return_data = []
    for index, item in enumerate(data):
        appendable_data = appendable_data + f'{item},'

        if index == number:
            con_data = appendable_data.split(",")
            con_data.pop()
            return_data.append(str(con_data).replace("[", "(").replace("]",")"))
            appendable_data=""
    return return_data
