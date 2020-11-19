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

from .online_scrapping import OnlineScrapping
from .online_signalr import OnlineSignalR
from .exceptions import DataException, SessionException, ServerException
from . import online_helper as helper

class Online:

    def __init__(self, auth, on_open=None, on_personal_portfolio=None,
        on_securities=None, on_options=None, on_repos=None, on_order_book=None,
        on_error=None, on_close=None, proxy_url=None):
        """
        Class constructor.

        Parameters
        ----------
        auth : home_broker_session
            An object with the authentication information.
        on_open : function(self), optional
            Callable object which is called at opening the signalR connection.
            This function has one argument. The argument is the callable object.
        on_personal_portfolio : function(self, quotes), optional
            Callable object which is called when personal portfolio data is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_securities : function(self, quotes), optional
            Callable object which is called when security data is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_options : function(self, quotes), optional
            Callable object which is called when options data is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_repos : function(self, quotes), optional
            Callable object which is called when repo data is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_order_book : function(self, quotes), optional
            Callable object which is called when the order book data (level 2) is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_error : function(self, exception, connection_lost), optional
            Callable object which is called when we get error.
            This function has 3 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the exception object.
                The 3rd argument is if the connection was lost due to the error.
        on_close : function(self), optional
            Callable object which is called when closed the connection.
            This function has one argument. The argument is the callable object.
        proxy_url : str, optional
            The proxy URL with one of the following formats:
                - scheme://user:pass@hostname:port
                - scheme://user:pass@ip:port
                - scheme://hostname:port
                - scheme://ip:port

            Ex. https://john:doe@10.10.1.10:3128
        """

        self._on_open = on_open
        self._on_personal_portfolio = on_personal_portfolio
        self._on_securities = on_securities
        self._on_options = on_options
        self._on_repos = on_repos
        self._on_order_book = on_order_book
        self._on_error = on_error
        self._on_close = on_close

        self._scrapping = OnlineScrapping(
            auth=auth,
            proxy_url=proxy_url)

        self._signalr = OnlineSignalR(
            auth=auth,
            on_open=self.__internal_on_open,
            on_personal_portfolio=self.__internal_on_personal_portfolio,
            on_securities=self.__internal_on_securities,
            on_options=self.__internal_on_options,
            on_repos=self.__internal_on_repos,
            on_order_book=self.__internal_on_order_book,
            on_error=self.__internal_on_error,
            on_close=self.__internal_on_close,
            proxy_url=proxy_url)

        # Used to keep tracking of personal portfolio subscriptions
        self.__personal_portfolio_groups = []

########################
#### PUBLIC METHODS ####
########################
    def connect(self):
        """
        Connects to the signalR server to receive quotes information.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the connection is already open or the user is not logged in.
        """

        if self._signalr.is_connected:
            raise SessionException('Connection is already open.')

        self._signalr.connect()

    def disconnect(self):
        """
        Disconnects from the signalR server.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the connection is not open or the user is not logged in.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open.')

        self._signalr.disconnect()

    def is_connected(self):
        """
        Returns if the signalR server is connected
        """

        return self._signalr.is_connected

    def subscribe_personal_portfolio(self):
        """
        Subscribe to the personal portfolio.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        df_portfolio, df_order_book = self._scrapping.get_personal_portfolio()
        try:
            self.__internal_on_personal_portfolio(
                df_portfolio.copy(),
                df_order_book.copy())

        except Exception as ex:
            self.__internal_on_error(ex, False)

        for _, row in df_portfolio.reset_index().iterrows():
            settlement = helper.get_settlement_for_request(row['settlement'], row['symbol'])
            group_name = '{}*{}*fv'.format(row['symbol'], settlement)

            self._signalr.join_group(group_name)
            self.__personal_portfolio_groups += [group_name]

    def unsubscribe_personal_portfolio(self):
        """
        Unsubscribe from the personal portfolio.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        for group_name in self.__personal_portfolio_groups:
            self._signalr.quit_group(group_name)

        self.__personal_portfolio_groups = []

    def subscribe_securities(self, board, settlement):
        """
        Subscribe to the security board with the specified settlement.

        Parameters
        ----------
        board : str
            The name of the board to be retrieved
            Valid values: bluechips, general_board, cedears, government_bonds, short_term_government_bonds, corporate_bonds
        settlement : str
            The settlement of the board to be retrieved
            Valid values: spot, 24hs, 48hs

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        pyhomebroker.exceptions.DataException
            When the board is not assigned, the settlement is not assigned or is not valid.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        if not board:
            raise DataException('Board is not assigned')

        if not settlement:
            raise DataException('Settlement is not assigned')

        board = helper.get_board_for_request(board)
        settlement = helper.get_settlement_for_request(settlement)

        df = self._scrapping.get_securities(board, settlement)
        try:
            self.__internal_on_securities(df.copy())

        except Exception as ex:
            self.__internal_on_error(ex, False)

        group_name = '{}-{}'.format(board, settlement)
        self._signalr.join_group(group_name)

    def unsubscribe_securities(self, board, settlement):
        """
        Unsubscribe from the security board with the specified settlement.

        Parameters
        ----------
        board : str
            The name of the board to be retrieved
            Valid values: bluechips, general_board, cedears, government_bonds, short_term_government_bonds, corporate_bonds
        settlement : str
            The settlement of the board to be retrieved
            Valid values: spot, 24hs, 48hs

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        pyhomebroker.exceptions.DataException
            When the board is not assigned, the settlement is not assigned or is not valid.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        if not board:
            raise DataException('Board is not assigned')

        if not settlement:
            raise DataException('Settlement is not assigned')

        board = helper.get_board_for_request(board)
        settlement = helper.get_settlement_for_request(settlement)

        group_name = '{}-{}'.format(board, settlement)
        self._signalr.quit_group(group_name)

    def subscribe_options(self):
        """
        Subscribe to the options board.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        df = self._scrapping.get_options()
        try:
            self.__internal_on_options(df.copy())

        except Exception as ex:
            self.__internal_on_error(ex, False)

        self._signalr.join_group('opciones-')

    def unsubscribe_options(self):
        """
        Unsubscribe from the options board.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        self._signalr.quit_group('opciones-')

    def subscribe_repos(self):
        """
        Subscribe to the repos board.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        df = self._scrapping.get_repos()
        try:
            self.__internal_on_repos(df.copy())

        except Exception as ex:
            self.__internal_on_error(ex, False)

        self._signalr.join_group('cauciones-')

    def unsubscribe_repos(self):
        """
        Subscribe from the repos board.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        self._signalr.quit_group('cauciones-')

    def subscribe_order_book(self, symbol, settlement):
        """
        Subscribe to the order book (level 2) of the specified symbol and settlement.

        Parameters
        ----------
        symbol : str
            The asset symbol or the repo currency.
        settlement : str
            The settlement of the board to be retrieved.
            Valid values:
                options: None or empty string.
                repos: datetime in format %Y%m%d (YYYYMMDD).
                rest of securities: spot, 24hs, 48hs.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        pyhomebroker.exceptions.DataException
            When the symbol is not assigned, the settlement is not assigned or is not valid.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        if not symbol:
            raise DataException('Symbol is not assigned')

        symbol = symbol.upper()
        settlement = helper.get_settlement_for_request(settlement, symbol)

        df = self._scrapping.get_order_book(symbol, settlement)
        try:
            self.__internal_on_order_book(df.copy())

        except Exception as ex:
            self.__internal_on_error(ex, False)

        group_name = '{}*{}*cj'.format(symbol, settlement)
        self._signalr.join_group(group_name)

    def unsubscribe_order_book(self, symbol, settlement):
        """
        Unsubscribe from the order book (level 2) of the specified symbol and settlement.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
            If the connection is not open.
            If the connection or hub is not assigned.
        pyhomebroker.exceptions.DataException
            When the symbol is not assigned, the settlement is not assigned or is not valid.
        """

        if not self._signalr.is_connected:
            raise SessionException('Connection is not open')

        if not symbol:
            raise DataException('Symbol is not assigned')

        if not settlement:
            raise DataException('Settlement is not assigned')

        symbol = symbol.upper()
        settlement = helper.get_settlement_for_request(settlement, symbol)

        group_name = '{}*{}*cj'.format(symbol, settlement)
        self._signalr.quit_group(group_name)

###########################
#### SIGNALR CALLBACKS ####
###########################
    def __internal_on_open(self):

        if self._on_open:
            self._on_open(self)

    def __internal_on_personal_portfolio(self, portfolio_quotes, order_book_quotes):

        if self._on_personal_portfolio and (not portfolio_quotes.empty or not order_book_quotes.empty):
            self._on_personal_portfolio(self, portfolio_quotes, order_book_quotes)

    def __internal_on_securities(self, quotes):

        if self._on_securities and not quotes.empty:
            self._on_securities(self, quotes)

    def __internal_on_options(self, quotes):

        if self._on_options and not quotes.empty:
            self._on_options(self, quotes)

    def __internal_on_repos(self, quotes):

        if self._on_repos and not quotes.empty:
            self._on_repos(self, quotes)

    def __internal_on_order_book(self, quotes):

        if self._on_order_book and not quotes.empty:
            self._on_order_book(self, quotes)

    def __internal_on_error(self, exception, connection_lost):

        if self._on_error:
            try:
                self._on_error(self, exception, connection_lost)
            except:
                pass

    def __internal_on_close(self):

        if self._on_close:
            self._on_close(self)
