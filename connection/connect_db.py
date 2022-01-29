import os
import pyodbc

from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


load_dotenv()


def open_connection(server_type):
    if server_type == 'source':
        server = os.getenv("source_server")
        database = os.getenv("source_database")
    else:
        server = os.getenv("destination_server")
        database = os.getenv("destination_database")

    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server +
        ';DATABASE=' + database +
        ';UID=' + os.getenv("db_username") +
        ';PWD=' + os.getenv("db_password"))

    connection.cursor()
    logger.info(f"CONNECTION SUCCESSFULLY {server_type}")
    return connection
