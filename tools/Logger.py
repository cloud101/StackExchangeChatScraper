__author__ = 'lucas'
from config  import LOG_LEVEL
import logging
logging.basicConfig()

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    return logger