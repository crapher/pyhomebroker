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

from ..common import user_agent

import datetime

import requests as rq
import pandas as pd
import numpy as np

class History:

    # Difference between UTC & Argentina Time Zone
    __hours = 3

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
                - scheme://ip:port

            Ex. https://john:doe@10.10.1.10:3128
        """

        self._proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None
        self._auth = auth

########################
#### PUBLIC METHODS ####
########################
    def get_daily_history(self, symbol, from_date, to_date):
        """
        Returns the historical quotes of the specified ticker narroweed by the date.

        Parameters
        ----------
        symbol : str
            The name of the symbol used to retrieve the information.
        from_date : datetime
            The start date used to filter the information.
        to_date : datetime
            The end date used to filter the information.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.
        """

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        url = '{}/HistoricoPrecios/history?symbol={}&resolution=D&from={}&to={}'.format(
            self._auth.broker['page'],
            symbol.upper(),
            self.__convert_datetime_to_epoch(from_date),
            self.__convert_datetime_to_epoch(to_date))

        resp = rq.get(url, headers=headers, cookies=self._auth.cookies, proxies=self._proxies)
        resp.raise_for_status()
        resp = resp.json()

        df = pd.DataFrame({'date': resp['t'], 'open': resp['o'], 'high': resp['h'], 'low': resp['l'], 'close': resp['c'], 'volume': resp['v']})
        df.date = pd.to_datetime(df.date, unit='s').dt.date
        df.volume = df.volume.astype(int)

        return df

    def get_intraday_history(self, symbol, from_date=None, to_date=None):
        """
        Returns the historical quotes of the specified ticker narroweed by the date.

        Parameters
        ----------
        symbol : str
            The name of the symbol used to retrieve the information.
        from_date : datetime
            The start date (Argentina Time Zone) used to filter the information.
        to_date : datetime
            The end date (Argentina Time Zone) used to filter the information.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.
        """

        if not self._auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        if from_date == None:
            from_date = datetime.date.today()

        if to_date == None:
            to_date = from_date + datetime.timedelta(days=1)

        from_date = from_date + datetime.timedelta(seconds=self.__hours * 3600)
        to_date = to_date + datetime.timedelta(seconds=self.__hours * 3600)

        url = '{}/Intradiario/history?symbol={}&resolution=1&from={}&to={}'.format(
            self._auth.broker['page'],
            symbol.upper(),
            self.__convert_datetime_to_epoch(from_date),
            self.__convert_datetime_to_epoch(to_date))

        resp = rq.get(url, headers=headers, cookies=self._auth.cookies, proxies=self._proxies)
        resp.raise_for_status()
        resp = resp.json()

        df = pd.DataFrame({'date': resp['t'], 'open': resp['o'], 'high': resp['h'], 'low': resp['l'], 'close': resp['c'], 'volume': resp['v']})
        df.date = pd.to_datetime(df.date, unit='s') - pd.DateOffset(seconds=self.__hours * 3600)
        df.volume = df.volume.astype(int)

        return df

#########################
#### PRIVATE METHODS ####
#########################
    def __convert_datetime_to_epoch(self, dt):

        if isinstance(dt, str):
            dt = datetime.datetime.strptime(dt, '%Y-%m-%d')

        dt_zero = datetime.date(1970, 1, 1)
        time_delta = dt - dt_zero
        return int(time_delta.total_seconds())
        