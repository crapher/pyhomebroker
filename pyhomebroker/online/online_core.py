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

from ..common import convert_to_numeric_columns, DataException

from abc import ABCMeta, abstractmethod
from datetime import datetime

import pandas as pd
import numpy as np

class OnlineCore(object, metaclass=ABCMeta):
    
    __settlements_int_map = {
        '1': 'spot',
        '2': '24hs',
        '3': '48hs'}

    __call_put_map = {
        0: '',
        1: 'CALL',
        2: 'PUT'}

    __group_map = {
        'accionesLideres': 'bluechips',
        'panelGeneral': 'general_board',
        'cedears': 'cedears',
        'rentaFija': 'government_bonds',
        'letes': 'short_term_government_bonds',
        'obligaciones': 'corporate_bonds'}

    __personal_portfolio_index = ['symbol', 'settlement']
    __personal_portfolio_columns = ['symbol', 'settlement', 'bid_size', 'bid', 'ask', 'ask_size', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'expiration', 'strike', 'kind', 'underlying_asset']
    __empty_personal_portfolio = pd.DataFrame(columns=__personal_portfolio_columns).set_index(__personal_portfolio_index)

    __securities_index = ['symbol', 'settlement']
    __securities_columns = ['symbol', 'settlement', 'bid_size', 'bid', 'ask', 'ask_size', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'group']
    __empty_securities = pd.DataFrame(columns=__securities_columns).set_index(__securities_index)

    __options_index = ['symbol']
    __options_columns = ['symbol', 'bid_size', 'bid', 'ask', 'ask_size', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'expiration', 'strike', 'kind', 'underlying_asset']
    __empty_options = pd.DataFrame(columns=__options_columns).set_index(__options_index)

    __repos_index = ['symbol', 'settlement']
    __repos_columns = ['symbol', 'days', 'settlement', 'bid_amount', 'bid_rate', 'ask_rate', 'ask_amount', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime']
    __empty_repos = pd.DataFrame(columns=__repos_columns).set_index(__repos_index)

    __order_book_index = ['symbol', 'settlement', 'position']
    __order_book_buy_columns = ['position', 'bid_size', 'bid', 'bid_offers_count']
    __order_book_sell_columns = ['position', 'ask_size', 'ask', 'ask_offers_count']
    __order_book_columns = list(set(__order_book_index) | set(__order_book_buy_columns) | set(__order_book_sell_columns))
    __empty_order_book = pd.DataFrame(columns=__order_book_columns).set_index(__order_book_index)

############################
## PROCESS JSON DOCUMENTS ##
############################
    def process_personal_portfolio(self, data):

        if not data:
            return self.__empty_personal_portfolio.copy()

        df = pd.DataFrame(data)
        if df.empty:
            return self.__empty_personal_portfolio.copy()

        filter_columns = ['Symbol', 'Term', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate', 'MaturityDate', 'StrikePrice', 'PutOrCall', 'Issuer']
        numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_size', 'bid', 'ask_size', 'ask', 'previous_close', 'strike']
        numeric_options_columns = ['MaturityDate', 'StrikePrice']
        alpha_option_columns = ['PutOrCall', 'Issuer']

        df.TradeDate = pd.to_datetime(df.TradeDate, format='%Y%m%d', errors='coerce') + pd.to_timedelta(df.Hour, errors='coerce')
        df.loc[df.StrikePrice == 0, alpha_option_columns] = ''
        df.loc[df.StrikePrice == 0, numeric_options_columns] = np.nan
        df.MaturityDate = pd.to_datetime(df.MaturityDate, format='%Y%m%d', errors='coerce')
        df.PutOrCall = df.PutOrCall.apply(lambda x: self.__call_put_map[x] if x in self.__call_put_map else self.__call_put_map[0])
        df.Term = df.Term.apply(lambda x: self.__settlements_int_map[x] if x in self.__settlements_int_map else '')

        df = df[filter_columns].copy()
        df.columns = self.__personal_portfolio_columns

        df = convert_to_numeric_columns(df, numeric_columns)
        df = df.set_index(self.__personal_portfolio_index)

        return df

    def process_securities(self, df):

        filter_columns = ['Symbol', 'Term', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate', 'Panel']
        numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_size', 'bid', 'ask_size', 'ask', 'previous_close']

        if not df.empty:
            df.TradeDate = pd.to_datetime(df.TradeDate, format='%Y%m%d', errors='coerce') + pd.to_timedelta(df.Hour, errors='coerce')
            df.Term = df.Term.apply(lambda x: self.__settlements_int_map[x] if x in self.__settlements_int_map else '')
            df.Panel = df.Panel.apply(lambda x: self.__group_map[x] if x in self.__group_map else '')

            df = df[filter_columns].copy()
            df.columns = self.__securities_columns

            df = convert_to_numeric_columns(df, numeric_columns)
            df = df.set_index(self.__securities_index)
        else:
            df = self.__empty_securities.copy()

        return df

    def process_options(self, df):

        filter_columns = ['Symbol', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate', 'MaturityDate', 'StrikePrice', 'PutOrCall', 'Issuer']
        numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_size', 'bid', 'ask_size', 'ask', 'previous_close', 'strike']

        if not df.empty:
            df.TradeDate = pd.to_datetime(df.TradeDate, format='%Y%m%d', errors='coerce') + pd.to_timedelta(df.Hour, errors='coerce')
            df.MaturityDate = pd.to_datetime(df.MaturityDate, format='%Y%m%d', errors='coerce')
            df.PutOrCall = df.PutOrCall.apply(lambda x: self.__call_put_map[x] if x in self.__call_put_map else self.__call_put_map[0])

            df = df[filter_columns].copy()
            df.columns = self.__options_columns

            df = convert_to_numeric_columns(df, numeric_columns)
            df = df[df.strike > 0].copy() # Remove non options rows

            df = df.set_index(self.__options_index)
        else:
            df = self.__empty_options.copy()

        return df

    def process_repos(self, df):

        filter_columns = ['Symbol', 'CantDias', 'Term', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate']
        numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_amount', 'bid_rate', 'ask_rate', 'ask_amount', 'previous_close']

        if not df.empty:
            df.TradeDate = pd.to_datetime(df.TradeDate, format='%Y%m%d', errors='coerce') + pd.to_timedelta(df.Hour, errors='coerce')

            df = df[filter_columns].copy()
            df.columns = self.__repos_columns

            df = convert_to_numeric_columns(df, numeric_columns)
            df = df.set_index(self.__repos_index)
        else:
            df = self.__empty_repos.copy()

        return df

    def process_order_book(self, name, settlement, df_buy, df_sell):

        filter_buy_columns = ['Pos', 'BuyQuantity', 'BuyPrice', 'NumberOfOrders']
        filter_sell_columns = ['Pos', 'SellQuantity', 'SellPrice', 'NumberOfOrders']

        df = pd.DataFrame(columns=self.__order_book_index)
        df.position = range(1, 6)
        df.symbol = name
        df.settlement = self.__settlements_int_map[settlement] if settlement in self.__settlements_int_map else settlement

        df_buy = df_buy[filter_buy_columns] if not df_buy.empty else pd.DataFrame(columns=filter_buy_columns)
        df_buy.columns = self.__order_book_buy_columns
        df = df.merge(df_buy, on=['position'], how='left')

        df_sell = df_sell[filter_sell_columns] if not df_sell.empty else pd.DataFrame(columns=filter_sell_columns)
        df_sell.columns = self.__order_book_sell_columns
        df = df.merge(df_sell, on=['position'], how='left')

        df = convert_to_numeric_columns(df, list(set(self.__order_book_buy_columns) | set(self.__order_book_sell_columns)))
        df = df.set_index(self.__order_book_index)

        return df

    def process_order_books(self, data):

        if not data:
            return self.__empty_order_book.copy()

        df = self.__empty_order_book.copy()

        for item in data:

            if item['StockDepthBox'] and item['StockDepthBox']['PriceDepthBox']:
                df_buy = pd.DataFrame(item['StockDepthBox']['PriceDepthBox']['BuySide'])
                df_sell = pd.DataFrame(item['StockDepthBox']['PriceDepthBox']['SellSide'])
            else:
                df_buy = pd.DataFrame()
                df_sell = pd.DataFrame()

            order_book = self.process_order_book(item['Symbol'], item['Term'], df_buy, df_sell)
            df = order_book if df.empty else pd.concat([df, order_book])

        return df
