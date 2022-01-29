import logging
from connection.connect_db import open_connection
from query_execution.match import match_data
from query_execution.batting import batting_data
from query_execution.bowling import bowling_data
from query_execution.fow import fow_data
from query_execution.powerplay import powerplay_data
from query_execution.segment1 import Segment
from query_execution.series import series_data
from query_execution.videos import videos_data
from query_execution.umpire import umpire_data
from query_execution.image import image_data
from utils import report
from utils.config import load_config
from utils.report import html_report

load_config()
logger = logging.getLogger(__name__)
report.init()

source_connection = open_connection("source")
destination_connection = open_connection("destination")

#series_data(source_connection, destination_connection)

#match_data(source_connection, destination_connection)

#videos_data(source_connection, destination_connection)

#batting_data(source_connection, destination_connection)

#bowling_data(source_connection, destination_connection)

#powerplay_data(source_connection, destination_connection)

#fow_data(source_connection, destination_connection)

#umpire_data(source_connection, destination_connection)

#image_data(source_connection, destination_connection)


Segment(source_connection, destination_connection).segment1_data()



logger.info(f'Final report => {report.Report}')
html_report()
