__author__ = 'lucas'
from scraper.Transcript import TranscriptScraper
import requests
from database.Elastic import ElasticManager
from tools.Logger import get_logger
from time import sleep
import os

logger = get_logger("scrape_dmz")
scraper = TranscriptScraper(151)
#keep a list which contains all URLs we need to fetch and process
process_list = set()
#keep a list of URLs which have already been processed so we do not fetch the same page twice
process_list.add(scraper.get_first_day())
processed_list = list()
#change headers for SE so they know if I cause load
headers = {
            'User-Agent': 'ChatExchangeScraper - contact Lucas Kauffman',
                }


x = 0

try:
		for root, dirs, files in os.walk("/home/lucas/dmz"):
			for file in files:
				if file.endswith(".html"):
					 with open(os.path.join(root, file)) as FILE:
						 response = FILE.read()
						 #a monologue can contain several messages
						 monologues = scraper.extract_monologues(response)
						 messages = scraper.extract_messages_from_monologues(monologues)
                                                 count = response.count('id="message')
                                                 if len(messages) != count:
                                                     print "count = %s"%count+ " messages = %s"% len(messages) +" FILe="+file
   						 ElasticManager.index_messages(messages)
                                                 x = x +1
except Exception, e:
        logger.exception(e)

