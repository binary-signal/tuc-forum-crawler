#!/usr/bin/python
# -*- coding: utf-8 -*-

from requests import session
from mongo import MDB
from bs4 import BeautifulSoup
from time import sleep
import queue
import config
import logging
import signal
from datetime import datetime


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
        'cache-control': "no-cache"}

    # payload for post log in request
    __payload = {'logintype': 'login',
                 'pass': config.password,  # this is for password
                 'permalogin': '1',
                 'pid': '2',
                 'submit': 'Σύνδεση',
                 'tx_felogin_pi1[noredirect]': '0',
                 'user': config.username}  # this is for username

    __session = None  # holds session cookies
    __root_soup = None  # soup object of root url passed in the constructor
    __page_counter = 1
    __post_counter = 0
    __throttle = 2  # apply throttle limit every 2 pages
    __waitTime = 2  # wait time when throttling
    __max_pages = 3
    __max_posts = 50
    __LOG_FILENAME = 'www.tuc.gr.out'
    logging.basicConfig(filename=__LOG_FILENAME,
                        level=logging.DEBUG)

    def __init__(self, root_forum_url, username, passw):

        self.__root_forum_url = root_forum_url
        self.__cur_url = root_forum_url  # points to current url fetched
        self.__username = username
        self.__passw = passw
        logging.debug('try to logging to tuc.gr')
        self.__login()
        self.__root_soup = self.fetch_n_soup(self.__root_forum_url,
                                             check_login=True)
        self.__q = queue.Queue()
        self.db = MDB(port=config.db_port, host=config.db_host,
                      dbname=config.db_name, dbcollection=config.db_collection)

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
                logging.debug(
                    'Download limit reached posts:{} pages:{}'.format(self.__post_counter, self.__page_counter))
                print("Download limit reached posts: {} pages:{}".format(self.__post_counter, self.__page_counter))
                self.store_to_db()
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
                logging.debug('Throttling limit reached : 5 sec sleep ..zZzZ')
                print("\t...zZzZ...\t...zZzZ...")
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
        logging.debug("parse root section url: %s".format(self.__cur_url))

        soup = self.fetch_n_soup(self.__cur_url, check_login=True)

        logging.debug('*Forum Page: {}'.format(self.__page_counter))
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
                logging.debug('\tfetching -> postid: {} title: {}'.format(self.__post_counter, title))

                self.__ShouldIthrottle()

                if self.has_reached_limits():
                    return

                post_soup = self.fetch_n_soup(url)

                texts = []
                posts_text = post_soup.find_all('td', class_='tx-mmforum-td tx-mmforum-pi1-listpost-text')
                for p in posts_text:
                    texts.append(p.text)

                post = {'title': title,
                        'url': url,
                        'body': "".join(texts),
                        'lang': post_soup.html.get('lang'),
                        'date': datetime.now().isoformat(),
                        'raw_responce': "*",
                        'processed': False}

                # print(json.dumps(post, indent=4, sort_keys=True))




                self.__q.put(post)

            print("db store...")
            self.store_to_db()
            if self.has_next_page(self.__root_soup):
                nextpage_url = self.get_next_page(self.__root_soup)

                # set current url to next page
                self.__set_cur_url(nextpage_url)

                # call parse
                self.__page_counter += 1
                self.parse_root_section_page()

    def store_to_db(self):
        while not self.__q.empty():
            post = self.__q.get()
            self.db.insert(post)

    def __store_to_file(self):
        while not self.__q.empty():
            post = self.__q.get()
            with open(post['url'], 'rw') as fp:
                fp.write(post)

    def has_next_page(self, soup):
        n = soup.find_all('li', class_='tx-pagebrowse-next')
        if len(n) is 0:
            return False
        if hasattr(n[0], n[0].a.text):
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

    print('TUC Forum crawler version 1.1\n')
    print("Crawl links summary:")
    keys = config.links.keys()
    for k in keys:
        print('[{}'.format(config.links[k]), end=']\n\n')

    for k in keys:
        root_forum_url = config.links[k]

        print('Start crawling: {}'.format(root_forum_url))
        tuc = TucForumCrawl(root_forum_url, config.username, config.password)
        try:
            tuc.parse_root_section_page()
            print('Finished crawling link: {}'.format(root_forum_url), end='\n\n')
        except KeyboardInterrupt:
            print("Keyboard interrupt received, stopping...")
            tuc.store_to_db()
            sleep(2)
            tuc.db.destroy()
            print('Bye Bye')
