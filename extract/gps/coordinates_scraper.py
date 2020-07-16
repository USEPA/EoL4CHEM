#!/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import ElementNotInteractableException
import pandas as pd
import sys, os, time
import re

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/..')
from common import config

class GPS_scraper:

    def __init__(self, address_city_state_zip):
        self._config = config()['web_sites']['GOOGLE_MAPS_COORDINATES']
        self._queries = self._config['queries']
        self._url = self._config['url']
        self.address_city_state_zip = address_city_state_zip


    def _searching_coodinates(self, search_parameter):
        self.search_bar = self._browser.find_element_by_xpath(self._queries['address'])
        self.search_bar.send_keys(search_parameter)
        self.button = self._browser.find_element_by_xpath(self._queries['get_button'])
        self._browser.execute_script("arguments[0].click();", self.button)
        time.sleep(3)


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
            address = str(row['ADDRESS']).strip()
            city = str(row['CITY']).strip()
            state = str(row['STATE']).strip()
            zip = str(row['ZIP']).strip()
            search_parameter = '{}, {}, {} {}, United States'.format(address, \
                                                                    city, \
                                                                    state, \
                                                                    zip)
            self._searching_coodinates(search_parameter)
            result = self._browser.current_url
            Coordinates = re.search('@(\-?[0-9]+\.?[0-9]*),(\-?[0-9]+\.?[0-9]*)', result)
            self.address_city_state_zip.loc[idx, 'LATITUDE'] = float(Coordinates.group(1))
            self.address_city_state_zip.loc[idx, 'LONGITUDE'] = float(Coordinates.group(2))
            try:
                self.search_bar.clear()
            except ElementNotInteractableException:
                self._browser.find_element_by_xpath(self._queries['distance_window']).click()
        self._browser.close()
        return self.address_city_state_zip
