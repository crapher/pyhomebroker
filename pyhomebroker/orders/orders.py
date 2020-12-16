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

from ..common import user_agent, convert_to_numeric_columns, SessionException, ServerException

import requests as rq
import pandas as pd
import numpy as np

class Orders:
    
    __settlements_orders_map = {
        'Contado': 'spot',
        '24 Hs.': '24hs',
        '48 Hs.': '48hs'}

    __order_status_map = {
        'Anulada': 'CANCELLED',
        'Pendiente': 'PENDING',
        'Recibida': 'OFFERED',
        'Cumplida': 'COMPLETED',
        'Parcial': 'PARTIAL',
        'Rechazada': 'REJECTED' # PENDING CHECK
    }
    
    __orders_status_index = ['order_number']
    __orders_status_columns = ['order_number', 'symbol', 'settlement', 'operation_type', 'size', 'price', 'remaining_size', 'datetime', 'status']
    __empty_orders_status = pd.DataFrame(columns=__orders_status_columns).set_index(__orders_status_index)

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

        self.__proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None
        self.__auth = auth

########################
#### PUBLIC METHODS ####
########################
    def get_orders_status(self, account_id):
        """
        Returns the orders status.

        Parameters
        ----------
        account_id : str
            The account identification used to retrieve the orders status.

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
        A dataframe with orders status.
        """

        data = self.__get_orders_status(account_id)
        data = data['Result'] if data and data['Result'] else None

        return self.__process_orders_status(data)

#########################
#### PRIVATE METHODS ####
#########################
    def __get_orders_status(self, account_id):

        if not self.__auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Consultas/GetConsulta'.format(self.__auth.broker['page'])

        payload = {
            'comitente': str(account_id),
            'consolida': '0',
            'proceso': '121',
            'tipo': None
        }

        response = rq.post(url, json=payload, headers=headers, cookies=self.__auth.cookies, proxies=self.__proxies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise ServerException(response['Error']['Descripcion'] or 'Unknown Error')

        return response
        
    def __process_orders_status(self, data):
    
        if not data:
            return self.__empty_orders_status
    
        filter_columns = ['NUME', 'TICK', 'PLAZ', 'TIPO', 'CANT', 'PCIO', 'REMN', 'FALT', 'ESTA']
        numeric_columns = ['order_number', 'size', 'price', 'remaining_size']
    
        df = pd.DataFrame()   
        for item in data:
    
            orders = item['listaDetalleTiker'][0]['ORDE'] if item['listaDetalleTiker'] and len(item['listaDetalleTiker']) > 0 and item['listaDetalleTiker'][0]['ORDE'] else []
            for order in orders:
    
                df_order = pd.DataFrame([order])
                df_order['REMN'] = pd.to_numeric(df_order.CANT)
                if order['APLI']:
                    df_operations = pd.DataFrame(order['APLI'])
                    df_operations.CANT = pd.to_numeric(df_operations.CANT)
                    df_order.REMN = df_order.REMN - df_operations.CANT.sum()
    
                df = df_order if df.empty else pd.concat([df, df_order])
    
        if not df.empty:
            df.FALT = pd.to_datetime(df.FALT, format='%d/%m/%y', errors='coerce') + pd.to_timedelta(df.HORA, errors='coerce')
            df.loc[(df.TICK.str.len() == 10), 'PLAZ'] = ''
            df.PLAZ = df.PLAZ.apply(lambda x: self.__settlements_orders_map[x] if x in self.__settlements_orders_map else x)
            df.TIPO = df.TIPO.apply(lambda x: 'BUY' if x == 'CPRA' else 'SELL')
            df.ESTA = df.ESTA.apply(lambda x: self.__order_status_map[x] if x in self.__order_status_map else x)
    
            df = df[filter_columns].copy()
            df.columns = self.__orders_status_columns
    
            df = convert_to_numeric_columns(df, numeric_columns)
    
            df = df.set_index(self.__orders_status_index).sort_index()
        else:
            df = self.__empty_orders_status
    
        return df
