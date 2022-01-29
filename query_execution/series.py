import re

import pandas as pd
import logging

from utils import report
from utils.generic import make_directory, read_sql_file

logger = logging.getLogger(__name__)


def series_data(source_conn, destination_conn):
    logger.info("series table data validation START")

    src_total_count, dst_total_count, src_diff_count, dst_diff_count = 0, 0, 0, 0

    # reading and executing sql queries on source table
    source_df = pd.read_sql(
        f'{read_sql_file("./query_resources/source/series.sql")}',
        source_conn).drop(columns=['MATCHCATEGORY']) #.apply(lambda x: x.astype(str).str.upper())

    for value in source_df["MAINTITLE"]:
        if re.match('^[0-9]*$', str(value.replace('_', '')).strip()):
            source_df['MAINTITLE'] = source_df['MAINTITLE'].replace(value, "OTHERS")
            src_total_count = src_total_count + len(source_df.index)

    # reading and executing sql queries on destination table
    destination_df = pd.read_sql(
        f'{read_sql_file("./query_resources/destination/series.sql")}',
        destination_conn) #.apply(lambda x: x.astype(str).str.upper())
    dst_total_count = dst_total_count + len(destination_df.index)

   # destination_df["matchcategory"] = source_df["matchcategory"]

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
            make_directory(f'./report/series')
            result.to_csv(f'./report/series/series.csv', index=False)

    except Exception as error:

        # adding csv file for both source and destination table
        make_directory(f'./report/series')
        source_sort_df.to_csv(f'./report/series/source.csv', index=False)
        destination_sort_df.to_csv(f'./report/series/destination.csv', index=False)
        logger.error(f"COUNT MISMATCH original error {error}")

    report.Report.append({"series": {"src_total_count": src_total_count, "dst_total_count": dst_total_count,
                                     "diff_count": src_total_count - dst_total_count}})
    logger.info(report.Report)
    logger.info("series table data validation END")
