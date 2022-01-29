import logging
from datetime import datetime


def load_config():
    FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(processName)s - %(name)s - %(message)s")

    LOGFILE =f'./log/{datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log'

    # http://docs.python.org/2/howto/logging-cookbook.html
    logger = logging.getLogger("")  # important! this is the *root logger*
    # all other loggers are derived from this one
    logger.setLevel(logging.DEBUG)  # *default* output level

    # StreamHandler sends to stderr by default
    h = logging.StreamHandler()
    h.setLevel(logging.INFO)  # output level for *this handler*
    h.setFormatter(FORMAT)

    # FileHandler sends to a named file
    h2 = logging.FileHandler(LOGFILE)
    h2.setFormatter(FORMAT)

    logger.addHandler(h)
    logger.addHandler(h2)

    logger.debug("configuration loaded")
