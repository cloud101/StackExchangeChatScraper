__author__ = 'lucas'
from config  import LOG_LEVEL
import logging
logging.basicConfig()
from config import LOGPATH
def get_logger(name):
    rootLogger = logging.getLogger(name)
    rootLogger.setLevel(LOG_LEVEL)
    fileHandler = logging.FileHandler("{0}/{1}.log".format(LOGPATH, "se_logger"))
    rootLogger.addHandler(fileHandler)
    consoleHandler = logging.StreamHandler()
    rootLogger.addHandler(consoleHandler)
    return rootLogger
