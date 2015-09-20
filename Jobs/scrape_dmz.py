__author__ = 'lucas'
from Scraper.Transcript import TranscriptScraper
import requests

x = TranscriptScraper(151)
process_list = list()
process_list.append( x.get_first_day() )
processed_list = list()
while process_list:
    url = process_list.pop()
    processed_list.append(url)
    response = requests.get(url)
    process_list.append( x.get_next_day(response.content) )
    for pager_url in x.get_pager(response.content):
        if pager_url not in processed_list:
            process_list.append(pager_url)

    print url
