import logging

import pandas as pd

from utils import report
from utils.generic import make_directory, read_sql_file

logger = logging.getLogger(__name__)

def bowling_data(source_cursor, destination_cursor):
    logger.info("Bowling table data validation START")
    # df means data frame
    match_ids = pd.read_sql(read_sql_file("./query_resources/source/bowling.sql"), source_cursor)["EX_MATCH_ID"]

    src_total_count, dst_total_count, src_diff_count, dst_diff_count = 0, 0, 0, 0
    for math_id in match_ids:
        # reading and executing sql queries on source table
        source_df = pd.read_sql(
            f'{read_sql_file("./query_resources/source/bowling.sql")} where Match_Id = \'{math_id}\'', source_cursor)

        src_total_count = src_total_count + len(source_df.index)

        # reading and executing sql queries on destination table
        destination_df = pd.read_sql(
            f'{read_sql_file("./query_resources/destination/bowling.sql")} and EX_Match_Id = \'{math_id}\'',
            destination_cursor)
        dst_total_count = dst_total_count + len(destination_df.index)

        # column_sort = ["S_Bat_Order", "S_Runs", "S_Minutes", "S_Fours", "S_Sixes", "S_Balls", "S_Innings"]
        # destination_df[column_sort] = destination_df[column_sort].apply(pd.to_numeric)

        df11 = source_df.sort_values(by=source_df.columns.tolist()).reset_index(drop=True)
        df21 = destination_df.sort_values(by=destination_df.columns.tolist()).reset_index(drop=True)
        make_directory(f'./report/bowling')
        try:
            result = df11.compare(df21)

            if result.size != 0:
                result.to_csv(f'./report/bowling/{math_id}.csv', index=False)
        except Exception as error:
            make_directory(f'./report/bowling/{math_id.replace(" ", "_")}')
            df11.to_csv(f'./report/bowling/{math_id.replace(" ", "_")}/source.csv', index=False)
            df21.to_csv(f'./report/bowling/{math_id.replace(" ", "_")}/destination.csv', index=False)
            logger.error(f"COUNT MISMARCH original error {error}")
    report.Report.append({"bowling": {"src_total_count": src_total_count, "dst_total_count": dst_total_count,
                                        "diff_count": src_total_count - dst_total_count}})
    logger.info(report.Report)
    logger.info("bowling table data validation END")
