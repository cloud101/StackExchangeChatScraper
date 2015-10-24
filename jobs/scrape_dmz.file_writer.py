__author__ = 'lucas'
from scraper.Transcript import TranscriptScraper
import requests
from database.Elastic import ElasticManager
from tools.Logger import get_logger
from time import sleep
import re
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
while process_list:
    sleep(2)
    try:
         url = process_list.pop()
         logger.info("Processing: %s"% url)
         processed_list.append(url)
         response = requests.get(url)
         next_day = scraper.get_next_day(response.content)
         if next_day not in processed_list:
              process_list.add(next_day)
         for pager_url in scraper.get_pager(response.content):
             if pager_url not in processed_list:
                 process_list.add(pager_url)
         with open('/home/lucas/dmz/%s.html'%x,'w') as FILE:
             FILE.write(response.content)
             FILE.close()
         x = x +1
    except Exception, e:
        logger.exception(e)

