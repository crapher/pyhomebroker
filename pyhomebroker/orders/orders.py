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

from ..common import user_agent, SessionException, ServerException, DataException

from datetime import datetime, timedelta
from threading import Lock

import requests as rq
import pandas as pd
import numpy as np

class Orders:

    __settlements_orders_map = {
        'Contado': 'spot',
        '24 Hs.': '24hs',
        '48 Hs.': '48hs'}

    __settlements_int_map = {
        'spot': '1',
        '24hs': '2',
        '48hs': '3'}

    __order_status_map = {
        'Anulada': 'CANCELLED',
        'Pendiente': 'PENDING',
        'Recibida': 'OFFERED',
        'Cumplida': 'COMPLETED',
        'Parcial': 'PARTIAL',
        'Rechazada': 'REJECTED' # PENDING CHECK
    }

    __orders_status_index = ['order_number']
    __orders_status_columns = ['order_number', 'symbol', 'settlement', 'operation_type', 'size', 'price', 'remaining_size', 'datetime', 'status', 'cancellable']
    __empty_orders_status = pd.DataFrame(columns=__orders_status_columns).set_index(__orders_status_index)

    __orders_send_lock = Lock()

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
        Get the orders status.

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
        orders = self.__filter_orders_from_json(data)

        return self.__process_orders(orders)

    def send_buy_order(self, symbol, settlement, price, size):
        """
        Send a buy order to the market.

        Parameters
        ----------
        symbol : str
            The asset symbol.
        settlement : str
            The settlement of the board to be retrieved.
            Valid values:
                options: None or empty string.
                rest of securities: spot, 24hs, 48hs.
        price : numeric
            The price to buy.
        size : numeric
            The quantity to buy.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        pyhomebroker.exceptions.DataException
            When one of the parameters is invalid.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.

        Returns
        -------
        The order number.
        """

        if size <= 0:
            raise DataException('Size is not valid')

        with self.__orders_send_lock:

            self.__send_order_validation(symbol, settlement, price, size)
            return self.__send_order_confirmation()

    def send_sell_order(self, symbol, settlement, price, size):
        """
        Send a sell order to the market.

        Parameters
        ----------
        symbol : str
            The asset symbol.
        settlement : str
            The settlement of the board to be retrieved.
            Valid values:
                options: None or empty string.
                rest of securities: spot, 24hs, 48hs.
        price : numeric
            The price to sell.
        size : numeric
            The quantity to sell.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        pyhomebroker.exceptions.DataException
            When one of the parameters is invalid.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.

        Returns
        -------
        The order number.
        """

        if size <= 0:
            raise DataException('Size is not valid')

        with self.__orders_send_lock:

            self.__send_order_validation(symbol, settlement, price, -size)
            return self.__send_order_confirmation()

    def cancel_order(self, account_id, order_number):
        """
        Cancel an order by number.

        Parameters
        ----------
        account_id : str
            The account identification used to retrieve the orders status.
        order_number : numeric
            The order number.

        Raises
        ------
        pyhomebroker.exceptions.SessionException
            If the user is not logged in.
        pyhomebroker.exceptions.ServerException
            When the server returns an error in the response.
        pyhomebroker.exceptions.DataException
            When one of the parameters is invalid.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.
        """

        data = self.__get_orders_status(account_id)
        orders = self.__filter_orders_from_json(data)

        for order in orders:

            if order['NUME'] == str(order_number):

                if not order['CanCancel']:
                    raise DataException("Order {} is not cancellable".format(order_number))

                with self.__orders_send_lock:

                    self.__send_cancel_validation(order['CESP'], order['TICK'], order['CANT'], order['PCIO'], order['IMPO'], order['FVTO'], order['TIPO'], order['PLAZ'], order['NUME'])
                    self.__send_cancel_confirmation()

                return

        raise DataException("Order {} not found".format(order_number))

    def cancel_all_orders(self, account_id):
        """
        Cancel all the cancellable orders.

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
        pyhomebroker.exceptions.DataException
            When one of the parameters is invalid.
        requests.exceptions.HTTPError
            There is a problem related to the HTTP request.
        """

        data = self.__get_orders_status(account_id)
        orders = self.__filter_orders_from_json(data)

        for order in orders:

            if not order['CanCancel']:
                continue

            with self.__orders_send_lock:

                self.__send_cancel_validation(order['CESP'], order['TICK'], order['CANT'], order['PCIO'], order['IMPO'], order['FVTO'], order['TIPO'], order['PLAZ'], order['NUME'])
                self.__send_cancel_confirmation()

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

        if not response['Result']:
            return [] # Response without result means that there are not orders in the list.

        return response['Result']

    def __filter_orders_from_json(self, data):

        result = []

        for item in data:

            if 'listaDetalleTiker' in item and item['listaDetalleTiker']:
                for detail in item['listaDetalleTiker']:

                    result = result + detail['ORDE']

        return result

    def __process_orders(self, orders):

        if not orders:
            return self.__empty_orders_status

        filter_columns = ['NUME', 'TICK', 'PLAZ', 'TIPO', 'CANT', 'PCIO', 'REMN', 'FALT', 'ESTA', 'CanCancel']
        numeric_columns = ['order_number', 'size', 'price', 'remaining_size']

        df = pd.DataFrame()

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

            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col].apply(lambda x: np.nan if x == '-' else x))

            df = df.set_index(self.__orders_status_index).sort_index()
        else:
            df = self.__empty_orders_status

        return df

    def __send_order_validation(self, symbol, settlement, price, size):

        if not self.__auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        if price <= 0:
            raise DataException('Price is not valid')

        if size != int(size):
            raise DataException('Size is not valid')

        if len(symbol) == 10: # Options
            settlement = '24hs'

        symbol = str.upper(symbol)
        settlement = str.lower(settlement)

        if not (settlement in self.__settlements_int_map):
            raise DataException('settlement is not valid')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Order/ValidarCargaOrdenAsync'.format(self.__auth.broker['page'])

        curr_time = datetime.utcnow() + timedelta(hours=-3)
        date_valid = curr_time if curr_time.hour <= 17 else curr_time + timedelta(days=1)

        payload = {
            'NombreEspecie': symbol,
            'Cantidad': str(abs(size)),
            'Precio': str(price).replace('.',','),
            'Importe': '',
            'DateValid': date_valid.strftime('%d/%m/%Y'),
            'OptionTipo': 1 if size > 0 else 2,
            'OptionTipoPlazo': self.__settlements_int_map[settlement]
        }

        response = rq.post(url, json=payload, headers=headers, cookies=self.__auth.cookies, proxies=self.__proxies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise ServerException(response['Error']['Descripcion'] or 'Unknown Error')

        if not response['Result']['ResponseOrden']['Verified']:
            raise ServerException(response['Result']['ResponseOrden']['ErrorMessage'] or 'Order not verified by server')

    def __send_order_confirmation(self):

        if not self.__auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Order/EnviarOrdenConfirmadaAsyc'.format(self.__auth.broker['page'])

        response = rq.post(url, headers=headers, cookies=self.__auth.cookies, proxies=self.__proxies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise ServerException(response['Error']['Descripcion'] or 'Unknown Error')

        if not response['Result']['ResponseOrden']['Accepted'] and \
            'HasReconfirmacion' in response['Result']['ResponseOrden'] and \
            response['Result']['ResponseOrden']['HasReconfirmacion']:
            response = self.__send_order_reconfirmation()

        if not response['Result']['ResponseOrden']['Accepted']:
            raise ServerException('Order not accepted by server.  Error: {}'.format(response['Result']['ResponseOrden']['ErrorMessage'] or 'Unknown Error'))

        return response['Result']['ResponseOrden']['Orden']['NroOrden']

    def __send_order_reconfirmation(self):

        if not self.__auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Order/EnviarOrdenReconfirmada'.format(self.__auth.broker['page'])

        response = rq.post(url, headers=headers, cookies=self.__auth.cookies, proxies=self.__proxies)
        response.raise_for_status()

        return response.json()

    def __send_cancel_validation(self, symbol_id, symbol, size, price, amount, date_valid, operation, settlement, order_number):

        if not self.__auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Order/EnviarCancelacionAsyc'.format(self.__auth.broker['page'])

        payload = {
            'especie': str(symbol_id),
            'Ticker': symbol,
            'Cantidad': '{:.2f}'.format(float(size)).replace('.',','),
            'Precio': '{:.2f}'.format(float(price)).replace('.',','),
            'Importe': '{:.2f}'.format(float(amount)).replace('.',','),
            'DateValid': date_valid,
            'OptionTipo': operation,
            'OptionTipoPlazo': settlement,
            'Numero': order_number,
        }

        response = rq.post(url, json=payload, headers=headers, cookies=self.__auth.cookies, proxies=self.__proxies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise ServerException(response['Error']['Descripcion'] or 'Unknown Error')

    def __send_cancel_confirmation(self):

        if not self.__auth.is_user_logged_in:
            raise SessionException('User is not logged in')

        headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = '{}/Order/EnviarOrdenCanceladaAsyc'.format(self.__auth.broker['page'])

        response = rq.post(url, headers=headers, cookies=self.__auth.cookies, proxies=self.__proxies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise ServerException(response['Error']['Descripcion'] or 'Unknown Error')