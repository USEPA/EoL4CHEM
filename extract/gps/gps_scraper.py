#!/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import sys, os, time
import re

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/..')
from common import config

'3345 Van Horn Road, Fairbanks, AK 99709, United States of America'

class GPS_scraper:

    def __init__(self, address_city_state_zip):
        self._config = config()['web_sites']['GPS']
        self._queries = self._config['queries']
        self._url = self._config['url']
        self.address_city_state_zip = address_city_state_zip


    def browsing(self):
        options = Options()
        options.headless = True
        options.add_argument('--disable-notifications')
        options.add_argument('--no-sandbox')
        options.add_argument('--verbose')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument("--log-level=3")
        options.add_argument('--hide-scrollbars')
        self._browser = webdriver.Chrome(ChromeDriverManager().install(), chrome_options = options)
        self._browser.get(self._url)
        self.address_city_state_zip['LONGITUDE'] = None
        self.address_city_state_zip['LATITUDE'] = None
        for idx, row in self.address_city_state_zip.iterrows():
            address = row['ADDRESS']
            city = row['CITY']
            state = row['STATE']
            zip = row['ZIP']
            search_parameter = '{}, {}, {} {}, United States'.format(address, \
                                                                                city, \
                                                                                state,
                                                                                zip)
            search_bar = self._browser.find_element_by_xpath(self._queries['address'])
            search_bar.send_keys(search_parameter)
            button = self._browser.find_element_by_xpath(self._queries['get_button'])
            self._browser.execute_script("arguments[0].click();", button)
            time.sleep(5)
            url = self._browser.current_url
            result = re.search('@(\-?[0-9]+\.?[0-9]*),(\-?[0-9]+\.?[0-9]*)', url)
            self.address_city_state_zip['LATITUDE'] = result.group(1)
            self.address_city_state_zip['LONGITUDE'] = result.group(2)
            current_url, result =  None, None
            search_bar.clear()
        self._browser.close()
        return self.address_city_state_zip
