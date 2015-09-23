__author__ = 'lucas'

from elasticsearch import Elasticsearch
from config import ELASTIC_CLUSTER
from tools.Logger import get_logger

logger = get_logger("Elastic")

class ESSessionManager(object):

    es_session = Elasticsearch(ELASTIC_CLUSTER)

    def __init__(self):
        pass


class ElasticManager(object):

    @staticmethod
    def index_messages(message_list):
        es = ESSessionManager().es_session
        for message in message_list:
            try:
                logger.debug(es.index(index="secse", doc_type="monologue", id=message["id"], body=message))
            except Exception,e:
                logger.exception(e)

    @staticmethod
    def index_message(message):
        es = ESSessionManager().es_session
        try:
            logger.debug(es.index(index="secse", doc_type="monologue", id=message["id"], body=message))
        except Exception,e:
            logger.exception(e)