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

from .common import brokers, BrokerNotSupportedException
from .home_broker_session import HomeBrokerSession
from .online import Online
from .history import History
from .orders import Orders

class HomeBroker:

    def __init__(self, broker_id, on_open=None, on_personal_portfolio=None,
        on_securities=None, on_options=None, on_repos=None, on_order_book=None,
        on_error=None, on_close=None, proxy_url=None):
        """
        Class constructor

        Parameters
        ----------
        broker_id : int
            The broker id registered in ByMA
        on_open : function(self), optional
            Callable object which is called at opening the signalR connection.
            This function has one argument. The argument is the callable object.
        on_personal_portfolio: function(self, quotes), optional
            Callable object which is called when personal portfolio data is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_securities: function(self, quotes), optional
            Callable object which is called when security data is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_options: function(self, quotes), optional
            Callable object which is called when options data is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_repos: function(self, quotes), optional
            Callable object which is called when repo data is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_order_book: function(self, quotes), optional
            Callable object which is called when the order book data (level 2) is received.
            This function has 2 arguments.
                The 1st argument is the callable object.
                The 2nd argument is the dataframe with the quotes.
        on_error : function(exception, connection_lost), optional
            Callable object which is called when we get error.
            This function has 2 arguments.
                The 1st argument is the exception object.
                The 2nd argument is if the connection was lost due to the error.
        on_close: function(self), optional
            Callable object which is called when closed the connection.
            This function has one argument. The argument is the callable object.
        proxy_url : str, optional
            The proxy URL with one of the following formats:
                - scheme://user:pass@hostname:port
                - scheme://user:pass@ip:port
                - scheme://hostname:port
                - scheme://ip:port

            Ex. https://john:doe@10.10.1.10:3128

        Raises
        ------
        pyhomebroker.exceptions.BrokerNotSupportedException
            The broker_id is not in the list of supported brokers
        """

        self._broker = self.__get_broker_data(broker_id)

        self.auth = HomeBrokerSession(
            broker=self._broker,
            proxy_url=proxy_url)

        self.online = Online(
            auth=self.auth,
            on_open=on_open,
            on_personal_portfolio=on_personal_portfolio,
            on_securities=on_securities,
            on_options=on_options,
            on_repos=on_repos,
            on_order_book=on_order_book,
            on_error=on_error,
            on_close=on_close,
            proxy_url=proxy_url)

        self.history = History(
            auth=self.auth,
            proxy_url=proxy_url)
            
        self.orders = Orders(
            auth=self.auth,
            proxy_url=proxy_url)

#########################
#### PRIVATE METHODS ####
#########################
    def __get_broker_data(self, broker_id):

        broker_data = [broker for broker in brokers if broker['broker_id'] == broker_id]

        if not broker_data:
            supported_brokers = ''.join([str(broker['broker_id']) + ', ' for broker in brokers])[0:-2]
            raise BrokerNotSupportedException('Broker not supported.  Brokers supported: {}.'.format(supported_brokers))

        return broker_data[0]
