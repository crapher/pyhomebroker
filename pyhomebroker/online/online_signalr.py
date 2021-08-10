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

from threading import Thread, Event, Lock

import requests as rq
import pandas as pd
import logging
import time

from signalr import Connection

import urllib.parse

class OnlineSignalR(OnlineCore):

    __worker_thread = None
    __worker_thread_event = None
    
    __personal_portfolio_queue_lock = Lock()
    __personal_portfolio_queue = []
    __securities_options_repos_queue_lock = Lock()
    __securities_options_repos_queue = []
    __order_book_queue_lock = Lock()
    __order_book_queue = []

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

            session.headers = {'User-Agent':user_agent}

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
                
                self.__worker_thread_event = Event()
                self.__worker_thread = Thread(target=self.__worker_thread_run)
                self.__worker_thread.start()

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

        self.__worker_thread_stop()
     
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
    def __worker_thread_run(self):

        while not self.__worker_thread_event.wait(0.1):
            
            with self.__personal_portfolio_queue_lock:
                data = self.__personal_portfolio_queue
                self.__personal_portfolio_queue = []
            self.__process_personal_portfolio(data)
            
            with self.__securities_options_repos_queue_lock:
                data = self.__securities_options_repos_queue
                self.__securities_options_repos_queue = []
            self.__process_securities_options_repos(data)

            with self.__order_book_queue_lock:
                data = self.__order_book_queue
                self.__order_book_queue = []
            self.__process_order_books(data)

    def __worker_thread_stop(self):
        
        if self.__worker_thread_event and not self.__worker_thread_event.is_set():
            self.__worker_thread_event.set()
            self.__worker_thread.join()
            self.__worker_thread_event = None
            self.__worker_thread = None
            
    def __process_personal_portfolio(self, data):
        
        try: #  Handle any exception processing the information or triggered by the user code
            if not self._on_personal_portfolio or len(data) == 0:
                return

            ts = time.time()
            
            # Remove duplicates from Json Document
            data_filter = {}
            for item in data:
                data_filter[item['Symbol'] + '-' + item['Term']] = item    
            data = list(data_filter.values())
            
            df_portfolio = self.process_personal_portfolio(data)
            ts_pp_process = time.time()

            df_order_book = self.process_order_books(data)
            ts_ob_process = time.time()
            
            self._on_personal_portfolio(df_portfolio, df_order_book)
            ts_event = time.time()

            logging.debug("[HOMEBROKER: SIGNALR] Performance [__process_personal_portfolio (P: {} - OB: {})]: (PP Proc: {:.3f}s - OB Proc: {:.3f}s - Notif: {:.3f}s)".format(
                len(df_portfolio.index),
                len(df_order_book.index),
                ts_pp_process - ts,
                ts_ob_process - ts_pp_process,
                ts_event - ts_ob_process))
        except Exception as ex:
            if self._on_error:
                try: # Catch user exceptions inside the except block (Inception Mode Activated :D)
                    self._on_error(ex, False)
                except:
                    pass

    def __process_securities_options_repos(self, data):
        
        try: # Handle any exception processing the information or triggered by the user code
            if len(data) == 0:
                return
            
            # Remove duplicates from Json Document
            data_filter = {}
            for item in data:
                data_filter[item['Symbol'] + '-' + item['Term']] = item    
            data = list(data_filter.values())
        
            df = pd.DataFrame(data) if data else pd.DataFrame()

            df_repo = df[df.Group == 'cauciones-'].copy()
            df_options = df[df.Group == 'opciones-'].copy()
            df_securities = df[(df.Group != 'cauciones-') & (df.Group != 'opciones-')].copy()

            if len(df_repo) and self._on_repos:
                ts = time.time()
                
                repos = self.process_repos(df_repo)
                ts_process = time.time()
                
                self._on_repos(repos)
                ts_event = time.time()
                
                logging.debug("[HOMEBROKER: SIGNALR] Performance [__process_securities_options_repos (R: {})]: (Proc: {:.3f}s - Notif: {:.3f}s)".format(
                    len(repos.index),
                    ts_process - ts,
                    ts_event - ts_process))

            if len(df_options) and self._on_options:
                ts = time.time()
                
                options = self.process_options(df_options)
                ts_process = time.time()

                self._on_options(options)
                ts_event = time.time()
                
                logging.debug("[HOMEBROKER: SIGNALR] Performance [__process_securities_options_repos (O: {})]: (Proc: {:.3f}s - Notif: {:.3f}s)".format(
                    len(options.index),
                    ts_process - ts,
                    ts_event - ts_process))
                    
            if len(df_securities) and self._on_securities:
                ts = time.time()
                
                securities = self.process_securities(df_securities)
                ts_process = time.time()

                self._on_securities(securities)
                ts_event = time.time()
                
                logging.debug("[HOMEBROKER: SIGNALR] Performance [__process_securities_options_repos (S: {})]: (Proc: {:.3f}s - Notif: {:.3f}s)".format(
                    len(securities.index),
                    ts_process - ts,
                    ts_event - ts_process))
        except Exception as ex:
            if self._on_error:
                try: # Catch user exceptions inside the except block (Inception Mode Activated :D)
                    self._on_error(ex, False)
                except:
                    pass

    def __process_order_books(self, data):
        
        try: # Handle any exception processing the information or triggered by the user code
            if not self._on_order_book or len(data) == 0:
                return
                
            ts = time.time()
            
            # Remove duplicates from Json Document
            data_filter = {}
            for item in data:
                data_filter[item['Symbol'] + '-' + item['Term']] = item    
            data = list(data_filter.values())
            
            order_books = self.process_order_books(data)
            ts_process = time.time()

            self._on_order_book(order_books)
            ts_event = time.time()

            logging.debug("[HOMEBROKER: SIGNALR] Performance [__process_order_books ({})]: (Proc: {:.3f}s - Notif: {:.3f}s)".format(
                len(data),
                ts_process - ts,
                ts_event - ts_process))
        except Exception as ex:
            if self._on_error:
                try: # Catch user exceptions inside the except block (Inception Mode Activated :D)
                    self._on_error(ex, False)
                except:
                    pass
                
#############################################
#### PRIVATE METHODS - SIGNALR CALLBACKS ####
#############################################
    def __internal_personal_portfolio(self, data):

        if not data:
            return
            
        if not isinstance(data, list):
            data = [data]
            
        with self.__personal_portfolio_queue_lock:
            self.__personal_portfolio_queue.extend(data)
     
    def __internal_securities_options_repos(self, data):

        if not data:
            return

        if not isinstance(data, list):
            data = [data]
            
        with self.__securities_options_repos_queue_lock:
            self.__securities_options_repos_queue.extend(data)

    def __internal_order_book(self, data):
        
        if not data:
            return
            
        if not isinstance(data, list):
            data = [data]
            
        with self.__order_book_queue_lock:
            self.__order_book_queue.extend(data)
 
    def __on_internal_exception(self, exception_type, value, traceback):
        
        self.__worker_thread_stop()
        
        if self._on_error:
            try: # Catch user exceptions inside the except block (Inception Mode Activated :D)
                self._on_error(exception_type(value), True)
            except:
                pass
