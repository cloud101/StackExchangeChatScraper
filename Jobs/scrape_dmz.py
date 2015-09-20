__author__ = 'lucas'
from Scraper.Transcript import TranscriptScraper
import requests
from Database.Elastic import ElasticManager

scraper = TranscriptScraper(151)
#keep a list which contains all URLs we need to fetch and process
process_list = list()
#keep a list of URLs which have already been processed so we do not fetch the same page twice
process_list.append(scraper.get_first_day())
processed_list = list()
while process_list:
    url = process_list.pop()
    print url
    processed_list.append(url)
    response = requests.get(url)
    process_list.append(scraper.get_next_day(response.content))
    for pager_url in scraper.get_pager(response.content):
        if pager_url not in processed_list:
            process_list.append(pager_url)
    # a monologue can contain several messages
    try:
        monologues = scraper.extract_monologues(response.content)
        messages = scraper.extract_messages_from_monologues(monologues)
        ElasticManager.index_messages(messages)
    except Exception,e:
        print e

