__author__ = 'lucas'
from bs4 import BeautifulSoup
import requests

BASE_URL = 'http://chat.stackexchange.com'
TRANSCRIPT = '/transcript/'

class TranscriptScraper(object):

    def __init__(self,room_id):
        self.room_id = room_id

    def get_first_day(self):
        response = requests.get(BASE_URL + TRANSCRIPT + str(self.room_id))
        soup = BeautifulSoup(response.content)
        main_div = soup.find("div",{"id":"main"})
        first_day_href = main_div.find('a')["href"]
        return BASE_URL+first_day_href


    def extract_messages_from_monologues(self, monologues_soups):
        """

        :param monologues_soups: BS4 soup where monologues have already been selected
        :return: returns a list containing a dictionary of messages
        """
        messages_data = list()
        for monologue_soup in monologues_soups:
            user_link, = monologue_soup.select('.signature .username a')
            user_id, user_name = self.user_id_and_name_from_link(user_link)

            message_soups = monologue_soup.select('.message')
            for message_soup in message_soups:
                message_id = int(message_soup['id'].split('-')[1])

                edited = bool(message_soup.select('.edits'))

                content = str(
                    message_soup.select('.content')[0]
                ).partition('>')[2].rpartition('<')[0].strip()

                star_data = self._get_star_data(
                    message_soup, include_starred_by_you=True)

                parent_info_soups = message_soup.select('.reply-info')

                if parent_info_soups:
                    parent_info_soup, = parent_info_soups
                    parent_message_id = int(
                        parent_info_soup['href'].partition('#')[2])
                else:
                    parent_message_id = None

                message_data = {
                    'id': message_id,
                    'content': content,
                    'owner_user_id': user_id,
                    'owner_user_name': user_name,
                    'edited': edited,
                    'parent_message_id': parent_message_id,
                    # TODO: 'time_stamp': ...
                }

                message_data.update(star_data)

                if not edited:
                    message_data['editor_user_id'] = None
                    message_data['editor_user_name'] = None
                    message_data['edits'] = 0

                messages_data.append(message_data)



        return messages_data


    def _get_star_data(self, root_soup, include_starred_by_you):
        """
        Gets star data indicated to the right of a message from a soup.
        """

        stars_soups = root_soup.select('.stars')

        if stars_soups:
            stars_soup, = stars_soups

            times_soup = stars_soup.select('.times')
            if times_soup and times_soup[0].text:
                stars = int(times_soup[0].text)
            else:
                stars = 1

            if include_starred_by_you:
                # some pages never show user-star, so we have to skip
                starred_by_you = bool(
                    root_soup.select('.stars.user-star'))

            pinned = bool(
                root_soup.select('.stars.owner-star'))

            if pinned:
                pins_known = False
            else:
                pins_known = True
                pinner_user_ids = []
                pinner_user_names = []
                pins = 0
        else:
            stars = 0
            if include_starred_by_you:
                starred_by_you = False

            pins_known = True
            pinned = False
            pins = 0
            pinner_user_ids = []
            pinner_user_names = []

        data = {
            'stars': stars,
            'starred': bool(stars),
            'pinned': pinned,
        }

        if pins_known:
            data['pinner_user_ids'] = pinner_user_ids
            data['pinner_user_names'] = pinner_user_names
            data['pins'] = pins

        if include_starred_by_you:
            data['starred_by_you'] = starred_by_you

        return data


    def get_pager(self,content):
        """
        The pager is a section which serves as a transcript spacer for when the transcript is too large to fit in a
        single page. The pager contains HREFs to subsections based on time.
        :param content: page content
        :return: list with hrefs to other transcript pages
        """

        soup = BeautifulSoup(content)
        pager = soup.find("div",{"class":"pager"})
        if pager:
            return [BASE_URL+a['href'] for a in pager.find_all('a')]
        else:
            return []

    @staticmethod
    def get_next_day(content):
        soup = BeautifulSoup(content)
        main_div = soup.find("div",{"id":"main"})
        next_day_href = main_div.find('link',{'rel':'next'})['href']
        if next_day_href:
            return BASE_URL + next_day_href
        else:
            return None


    def extract_monologues(self,content):
        soup = BeautifulSoup(content)
        monologues = soup.select('.monologue')
        return monologues


    @staticmethod
    def user_id_and_name_from_link(link_soup):
        user_name = link_soup.text
        user_id = int(link_soup['href'].split('/')[2])
        return user_id, user_name

    def get_owner(self,monologue):
        owner_soup = monologue.select('.username a')[0]
        owner_user_id, owner_user_name = (
            self.user_id_and_name_from_link(owner_soup))
        return owner_user_id,owner_user_name

    def get_content(self,monologue):
        return str(
            monologue.select('.content')[0]
        ).partition('>')[2].rpartition('<')[0].strip()
