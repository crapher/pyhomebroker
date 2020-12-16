#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Home Broker API - Market data downloader
# https://github.com/crapher/pyhomebroker.git
#
# Copyright 2020 Diego Degese
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from ..common import user_agent, DataException, SessionException, ServerException
from .online_core import OnlineCore

import requests as rq
import pandas as pd
import numpy as np

class OnlineScrapping(OnlineCore):

    def __init__(self, auth, proxy_url=None):
        """
        Class constructor.

        Parameters
        ----------
        auth : home_broker_session
            An object with the authentication information.
        proxy_url : str, optional
            The proxy URL with one of the following formats:
                - scheme://user:pass@hostname:port
                - scheme://user:pass@ip:port
                - scheme://hostname:port
                - schemeC

            Ex. https://john:doe@10.10.1.10:3128
        """

        self._proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None
        self._auth = auth

########################
#### PUBLIC METHODS ####
########################
    def get_personal_portfolio(self):
        """
        Returns the configured personal portfolio.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.

        Returns
        -------
        An empty dataframe or a dataframe with the quotes.
        """

        data = self.__get_personal_portfolio()
        data = data['Result'] if data and data['Result'] else None
        
        df_portfolio = self.process_personal_portfolio(data)
        df_order_book = self.process_order_books(data)

        return [df_portfolio, df_order_book]

    def get_securities(self, board, settlement):
        """
        Returns the security board specified by the name and settlement.

        Parameters
        ----------
        board : str
            The name of the board to be retrieved.
            Valid values: accionesLideres, panelGeneral, cedears, rentaFija, letes, obligaciones.
        settlement : int
            The settlement of the board to be retrieved.
            Valid values: 1, 2, 3.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        pyhomebroker.exceptions.DataException
            When the board name or the settlement is not valid.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.

        Returns
        -------
        An empty dataframe or a dataframe with the quotes.
        """

        data = self.__get_predefined_portfolio(board, settlement)
        df = pd.DataFrame(data['Result']['Stocks']) if data['Result'] and data['Result']['Stocks'] else pd.DataFrame()

        return self.process_securities(df)

    def get_options(self):
        """
        Returns the options board.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.

        Returns
        -------
        An empty dataframe or a dataframe with the quotes.
        """

        data = self.__get_predefined_portfolio('opciones')
        df = pd.DataFrame(data['Result']['Stocks']) if data['Result'] and data['Result']['Stocks'] else pd.DataFrame()

        return self.process_options(df)

    def get_repos(self):
        """
        Returns the repo board.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.

        Returns
        -------
        An empty dataframe or a dataframe with the repos.
        """

        data = self.__get_predefined_portfolio('cauciones')
        df = pd.DataFrame(data['Result']['Stocks']) if data['Result'] and data['Result']['Stocks'] else pd.DataFrame()

        return self.process_repos(df)

    def get_order_book(self, symbol, settlement=None):
        """
        Returns the order book specified by the name and settlement.

        Parameters
        ----------
        symbol : str
            The asset symbol or the repo currency.
        settlement : str
            The settlement of the board to be retrieved.
            Valid values:
                options: None or empty string.
                repos: datetime in format %Y%m%d (YYYYMMDD).
                rest of securities: 1, 2, 3.

        Raises
        ------
        pyhomebroker.exceptions.DataException
            When the settlement is not valid.
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.

        Returns
        -------
        A dataframe with quotes.
        """

        data = self.__get_asset(symbol, settlement)

        if data['Result'] and data['Result']['Stock'] and data['Result']['Stock']['StockDepthBox'] and data['Result']['Stock']['StockDepthBox']['PriceDepthBox']:
            df_buy = pd.DataFrame(data['Result']['Stock']['StockDepthBox']['PriceDepthBox']['BuySide'])
            df_sell = pd.DataFrame(data['Result']['Stock']['StockDepthBox']['PriceDepthBox']['SellSide'])
        else:
            df_buy = pd.DataFrame()
            df_sell = pd.DataFrame()

        return self.process_order_book(symbol, settlement, df_buy, df_sell)

#########################
#### PRIVATE METHODS ####
#########################
    def __get_personal_portfolio(self):

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Prices/GetFavoritos'.format(self._auth.broker['page'])

        response = rq.post(url, headers=headers, cookies=self._auth.cookies, proxies=self._proxies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise ServerException(response['Error']['Descripcion'] or 'Unknown Error')

        return response

    def __get_predefined_portfolio(self, board, settlement=None):

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Prices/GetByPanel'.format(self._auth.broker['page'])

        payload = {
            'panel': board,
            'term': settlement or ''
        }

        response = rq.post(url, json=payload, headers=headers, cookies=self._auth.cookies, proxies=self._proxies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise ServerException(response['Error']['Descripcion'] or 'Unknown Error')

        return response

    def __get_asset(self, symbol, settlement):

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Prices/GetByStock'.format(self._auth.broker['page'])

        payload = {
            'symbol': symbol,
            'term': settlement
        }

        response = rq.post(url, json=payload, headers=headers, cookies=self._auth.cookies, proxies=self._proxies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise ServerException(response['Error']['Descripcion'] or 'Unknown Error')

        return response
