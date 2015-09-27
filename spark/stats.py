__author__ = 'lucas'
import re


def count_word(sentence, word):
    return sentence.lower().count(word)



def extract_urls(sentence):
    return re.search("(?P<url>https?://[^\s]+)", sentence).group("url")


es_rdd.map(lambda a: (a[1]['owner_user_name'],count_word(a[1]['content'],'donut'))).filter(lambda x: len(x[1
                                                                                                    ]) > 0)