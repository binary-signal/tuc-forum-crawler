#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
from requests import session
import pprint
from mongo import MDB
from bs4 import BeautifulSoup
from time import sleep
import queue
import config


class TucForumCrawl:
    __login_url = 'https://www.tuc.gr/index.php?id=login'
    __headers = {
        'origin': "https://www.tuc.gr",
        'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
        'content-type': "application/x-www-form-urlencoded",
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        'dnt': "1",
        'referer': "https://www.tuc.gr/index.php?id=829",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "en-US,en;q=0.8",
        'cookie': "tuclightscookie=1; _ga=GA1.2.439343948.1473727072; _pk_ref.10.d786=%5B%22%22%2C%22%22%2C1482836181%2C%22https%3A%2F%2Fwww.google.gr%2F%22%5D; _pk_id.10.d786=1aaf9153bf67e5ba.1473892719.2.1482836234.1482836181.; __unam=102b87c-15ae9640260-6819ac9c-12; _pk_ref.1.d786=%5B%22%22%2C%22%22%2C1491234143%2C%22https%3A%2F%2Fwww.facebook.com%2F%22%5D; _pk_id.1.d786=d99478d52a29e818.1472830265.126.1491234143.1491234143.",
        'cache-control': "no-cache",
        'postman-token': "28b3a37e-839b-9d64-0caf-6cee9c332b10"}

    # payload for post log in request
    __payload = {'logintype': 'login',
                 'pass': config.password,   # this is for password
                 'permalogin': '1',
                 'pid': '2',
                 'submit': 'Σύνδεση',
                 'tx_felogin_pi1[noredirect]': '0',
                 'user': config.username}       #this is for username

    __root_forum_url = None
    __cur_url = None  # points to current url fetched
    __page_counter = 1
    __post_counter = 0
    __session = None  # holds session cookies
    __root_soup = None  # soup object of root url passed in the constructor
    __throttle = 2  # throttle limit
    __waitTime = 5  # wait time when throttling
    __max_pages = -1
    __max_posts = -1

    __db = MDB(port=27017, host='127.0.0.1', dbname='tuc', dbcollection='test')

    def __init__(self, root_forum_url, username, passw):

        self.__root_forum_url = root_forum_url
        self.__cur_url = root_forum_url
        self.__username = username
        self.__passw = passw
        # update payload with user credentials
        #self.__payload['username'] =username
        #self.__payload['pass'] = passw
        self.__login()
        self.__root_soup = self.fetch_n_soup(self.__root_forum_url, check_login=True)
        self.__q = queue.Queue()


    def set_max_pages(self, max_pages):
        self.__max_pages = max_pages

    def set_max_posts(self, max_posts):
        self.__max_posts = max_posts

    def set_throttle(self, t):
        self.__throttle = t

    def get_throttle(self):
        return self.__throttle

    def __zzz(self):
        sleep(self.__waitTime)

    def has_reached_limits(self):
        """check if limits reached for downloaded data,
        setting max_posts or max_pages to negative values
        will virtually disable  limit checking. Before returning
        True this function stores to db everything that's in the queue
         in order to avoid data loss.
        """
        if self.__max_posts <= 0 or self.__max_pages <= 0:
            return False
        else:
            if self.__page_counter >= self.__max_pages or self.__post_counter >= self.__max_posts:
                print("Download limit reached posts: {} pages:{}".format(self.__post_counter, self.__page_counter))
                self.__store_to_db()
                return True
            else:
                False

    def __ShouldIthrottle(self):
        """in order to avoid banning from server
        pause fetching data every two pages or 
        when post count is 20 for 5 seconds then resume
        """
        if self.__page_counter % self.__throttle == 0:
            if self.__post_counter % (self.__throttle * 10) == 0:
                print("\t...zzz...\t...zzz...")
                return self.__zzz()
            else:
                return False


    def __set_cur_url(self, new_url):
        self.__cur_url = new_url

    def __set_root_soup(self, new_soup):
        self.__root_soup = new_soup

    def __get_root_soup(self):
        return self.__root_soup

    def __login(self):
        """attempt to login at tuc.gr protected area
        sets cookie for requests in the future"""
        self.__session = session()
        self.__session.post(self.__login_url, data=self.__payload)
        return self.__session

    def still_logged(self, resp):
        if resp.find(self.__username):
            return True
        else:
            False

    def fetch_n_soup(self, url, check_login=False):
        """Fetches a url and returns a soup object
        if status code isn't 200 OK then return
        None
        """
        response = self.__session.get(url)
        if response.status_code == 200:
            if check_login:  # check if logged in
                if self.still_logged(response.text):
                    soup = BeautifulSoup(response.text, 'html.parser')
                    return soup
                else:
                    return None
            else:  # don't check log in just return page
                soup = BeautifulSoup(response.text, 'html.parser')
                return soup
        else:
            return None

    def parse_root_section_page(self):
        soup = self.fetch_n_soup(self.__cur_url, check_login=True)

        print('*Forum Page: {}'.format(self.__page_counter))

        if soup:

            # get all topics from table page
            # extract title and href
            topics = soup.find_all('span', class_='tx-mmforum-pi1-listtopic-topicname')
            for t in topics:  # for every post in root page
                # print("extracting titles and url from page")

                # extract post title and url first
                title = t.a.get('title')
                url = t.a.get('href')

                # fetch post
                self.__post_counter += 1
                # print('\tfetching -> postid: {} title: {}'.format(self.__post_counter, title))
                print("*", end='')
                self.__ShouldIthrottle()

                if self.has_reached_limits():
                    return
                response = self.__session.get(url)
                # print('ok')

                # check status code
                if response.status_code != 200:
                    print("error:::::")

                # feed post to bs
                # print('parsing post page')
                post_page = BeautifulSoup(response.text, 'html.parser')

                texts = []
                posts_text = post_page.find_all('td', class_='tx-mmforum-td tx-mmforum-pi1-listpost-text')
                for p in posts_text:
                    # print(p.text)

                    texts.append(p.text)

                post = {'title': title,
                        'url': url,
                        'body': "".join(texts),
                        'lang': post_page.html.get('lang')}

                self.__q.put(post)

            print("db store...")
            self.__store_to_db()
            if self.has_next_page(self.__root_soup):
                nextpage_url = self.get_next_page(self.__root_soup)

                # set current url to next page
                self.__set_cur_url(nextpage_url)

                # call parse
                self.__page_counter += 1
                self.parse_root_section_page()
            else:
                print('Finished !!!! ')

    def __store_to_db(self):
        while not self.__q.empty():
            post = self.__q.get()
            self.__db.insert(post)

    def has_next_page(self, soup):
        n = soup.find_all('li', class_='tx-pagebrowse-next')
        if n[0].a.text == "Επόμενη>":
            return True
        else:
            return False

    def get_next_page(self, soup):
        if self.has_next_page(soup):
            n = soup.find_all('li', class_='tx-pagebrowse-next')
            return n[0].a.get('href')
        else:
            return None


if __name__ == "__main__":

 db = MDB(port=config.db_port, host=config.db_host, dbname=config.db_name, dbcollection=config.db_collection)
 # db.search_by_attr('lang', 'el')
 print(db.num_of_docs()   )
 # db.destroy()


 root_forum_url = 'https://www.tuc.gr/index.php?id=news&tx_mmforum_pi1%5Baction%5D=list_topic&tx_mmforum_pi1%5Bfid%5D=5'

 tuc = TucForumCrawl(root_forum_url, config.username, config.password)
 tuc.parse_root_section_page()
