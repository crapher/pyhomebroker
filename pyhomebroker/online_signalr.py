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

from . import __user_agent__
from . import online_helper as helper
from .exceptions import DataException, SessionException, ServerException

import requests as rq
import pandas as pd

from signalr import Connection

import urllib.parse

class OnlineSignalR:

    def __init__(self, auth, on_open=None, on_personal_portfolio=None,
        on_securities=None, on_options=None, on_repos=None, on_order_book=None,
        on_error=None, on_close=None, proxy_url=None):
        """
        Class constructor.

        Parameters
        ----------
        auth : home_broker_session
            An object with the authentication information.
        on_open : function(), optional
            Callable object which is called at opening the signalR connection.
            This function has no argument.
        on_personal_portfolio : function(quotes), optional
            Callable object which is called when personal portfolio data is received.
            This function has 1 argument. The argument is the dataframe with the quotes.
        on_securities : function(quotes), optional
            Callable object which is called when security data is received.
            This function has 1 argument. The argument is the dataframe with the quotes.
        on_options : function(quotes), optional
            Callable object which is called when options data is received.
            This function has 1 argument. The argument is the dataframe with the quotes.
        on_repos : function(quotes), optional
            Callable object which is called when repo data is received.
            This function has 1 argument. The argument is the dataframe with the quotes.
        on_order_book : function(quotes), optional
            Callable object which is called when the order book data (level 2) is received.
            This function has 1 argument. The argument is the dataframe with the quotes.
        on_error : function(exception, connection_lost), optional
            Callable object which is called when we get error.
            This function has 2 arguments.
                The 1st argument is the exception object.
                The 2nd argument is if the connection was lost due to the error.
        on_close : function(), optional
            Callable object which is called when closed the connection.
            This function has no argument.
        proxy_url : str, optional
            The proxy URL with one of the following formats:
                - scheme://user:pass@hostname:port
                - scheme://user:pass@ip:port
                - scheme://hostname:port
                - scheme://ip:port

            Ex. https://john:doe@10.10.1.10:3128
        """

        self._proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None
        self._auth = auth
        self._on_open = on_open
        self._on_personal_portfolio = on_personal_portfolio
        self._on_securities = on_securities
        self._on_options = on_options
        self._on_repos = on_repos
        self._on_order_book = on_order_book
        self._on_error = on_error
        self._on_close = on_close

        self._connection = None
        self._hub = None

        self.is_connected = False

########################
#### PUBLIC METHODS ####
########################
    def connect(self):
        """
        Connects to the signalR server.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        """

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        url = '{}/signalr/hubs'.format(self._auth.broker['page'])

        with rq.Session() as session:
            rq.utils.add_dict_to_cookiejar(session.cookies, self._auth.cookies)

            if self._proxies:
                session.proxies.update(self._proxies)

            session.headers = {'User-Agent':__user_agent__}

            self._connection = Connection(url, session)
            self._hub = self._connection.register_hub('stockpriceshub')

            self._hub.client.on('broadcast', self.__internal_securities_options_repos)

            self._hub.client.on('sendStartStockFavoritos', self.__internal_personal_portfolio)
            self._hub.client.on('sendStockFavoritos', self.__internal_personal_portfolio)

            self._hub.client.on('sendStartStockPuntas', self.__internal_order_book)
            self._hub.client.on('sendStockPuntas', self.__internal_order_book)

            if self._on_error:
                self._connection.error += self._on_error

            self._connection.exception += self.__on_internal_exception
            self._connection.start()

            self.is_connected = self._connection.is_open

            if self.is_connected and self._on_open:
                self._on_open()

    def disconnect(self):
        """
        Disconnects from the signalR server.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection or hub is not assigned.
        """

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        if not self._connection or not self._hub:
            raise SessionException('Connection or hub is not assigned')

        if self._connection.is_open:
            self._connection.close()

        self._connection = None
        self._hub = None

        self.is_connected = False

        if self._on_close:
            self._on_close()

    def join_group(self, group_name):
        """
        Subscribe to a group to start receiving event notifications.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection or hub is not assigned.
            If the connection is not open.
        """

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        if not self._connection or not self._hub:
            raise SessionException('Connection or hub is not assigned')

        if not self._connection.is_open:
            raise SessionException('Connection is not open')

        self._hub.server.invoke('JoinGroup', group_name)

    def quit_group(self, group_name):
        """
        Unsubscribe from a group to stop receiving event notifications.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection or hub is not assigned.
            If the connection is not open.
        """

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        if not self._connection or not self._hub:
            raise SessionException('Connection or hub is not assigned')

        if not self._connection.is_open:
            raise SessionException('Connection is not open')

        self._hub.server.invoke('QuitGroup', group_name)

#########################
#### PRIVATE METHODS ####
#########################
    def __internal_personal_portfolio(self, data):

        try: #  Handle any exception processing the information or triggered by the user code
            if data and not isinstance(data, list):
                data = [data]

            df_portfolio = helper.process_personal_portfolio(data)
            df_order_book = helper.process_personal_portfolio_order_book(data)
            
            if self._on_personal_portfolio and data:
                self._on_personal_portfolio(df_portfolio, df_order_book)

        except Exception as ex:
            if self._on_error:
                try: # Catch user exceptions inside the except block (Inception Mode Activated :D)
                    self._on_error(ex, False)
                except:
                    pass

    def __internal_securities_options_repos(self, data):

        try: # Handle any exception processing the information or triggered by the user code
            df = pd.DataFrame(data) if data else pd.DataFrame()

            df_repo = df[df.Group == 'cauciones-']
            df_options = df[df.Group == 'opciones-']
            df_securities = df[(df.Group != 'cauciones-') & (df.Group != 'opciones-')]

            if len(df_repo) and self._on_repos:
                self._on_repos(helper.process_repos(df_repo))

            if len(df_options) and self._on_options:
                self._on_options(helper.process_options(df_options))

            if len(df_securities) and self._on_securities:
                self._on_securities(helper.process_securities(df_securities))

        except Exception as ex:
            if self._on_error:
                try: # Catch user exceptions inside the except block (Inception Mode Activated :D)
                    self._on_error(ex, False)
                except:
                    pass

    def __internal_order_book(self, data):

        try: # Handle any exception processing the information or triggered by the user code
            if self._on_order_book and data:
                symbol = data['Symbol']
                settlement = data['Term']

                if data['StockDepthBox'] and data['StockDepthBox']['PriceDepthBox']:
                    df_buy = pd.DataFrame(data['StockDepthBox']['PriceDepthBox']['BuySide'])
                    df_sell = pd.DataFrame(data['StockDepthBox']['PriceDepthBox']['SellSide'])
                else:
                    df_buy = pd.DataFrame()
                    df_sell = pd.DataFrame()

                self._on_order_book(helper.process_order_book(symbol, settlement, df_buy, df_sell))

        except Exception as ex:
            if self._on_error:
                try: # Catch user exceptions inside the except block (Inception Mode Activated :D)
                    self._on_error(ex, False)
                except:
                    pass

    def __on_internal_exception(self, exception_type, value, traceback):

        if self._on_error:
            try: # Catch user exceptions inside the except block (Inception Mode Activated :D)
                self._on_error(exception_type(value), True)
            except:
                pass
