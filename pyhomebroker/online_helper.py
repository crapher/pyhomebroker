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

import pandas as pd
import numpy as np

from datetime import datetime

from .exceptions import DataException

__settlements_int = {
    '1': 'spot',
    '2': '24hs',
    '3': '48hs'}

__settlements_str = {
    'spot': '1',
    '24hs': '2',
    '48hs': '3'}

__callput = {
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

############################
## PROCESS JSON DOCUMENTS ##
############################
def convert_to_numeric_columns(df, columns):

    for col in columns:
        df[col] = df[col].apply(lambda x: x.replace('.', '').replace(',','.') if isinstance(x, str) else x)
        df[col] = pd.to_numeric(df[col].apply(lambda x: np.nan if x == '-' else x))

    return df

def process_personal_portfolio(data):

    result_index = ['symbol', 'settlement']
    filter_columns = ['Symbol', 'Term', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate', 'MaturityDate', 'StrikePrice', 'PutOrCall', 'Issuer']
    result_columns = ['symbol', 'settlement', 'bid_size', 'bid', 'ask', 'ask_size', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'expiration', 'strike', 'kind', 'underlying_asset']
    numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_size', 'bid', 'ask_size', 'ask', 'previous_close', 'strike']
    numeric_options_columns = ['MaturityDate', 'StrikePrice']
    alpha_option_columns = ['PutOrCall', 'Issuer']

    df = pd.DataFrame(data if data else pd.DataFrame())

    if not df.empty:
        df.TradeDate = pd.to_datetime(df.TradeDate, format='%Y%m%d', errors='coerce') + pd.to_timedelta(df.Hour, errors='coerce')
        df.loc[df.StrikePrice == 0, alpha_option_columns] = ''
        df.loc[df.StrikePrice == 0, numeric_options_columns] = np.nan
        df.MaturityDate = pd.to_datetime(df.MaturityDate, format='%Y%m%d', errors='coerce')
        df.PutOrCall = df.PutOrCall.apply(lambda x: __callput[x] if x in __callput else __callput[0])
        df.Term = df.Term.apply(lambda x: __settlements_int[x] if x in __settlements_int else '')

        df = df[filter_columns].copy()
        df.columns = result_columns

        df = convert_to_numeric_columns(df, numeric_columns)
    else:
        df = pd.DataFrame(columns=result_columns)

    df = df.set_index(result_index)
    return df

def process_personal_portfolio_order_book(data):

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame()

    for item in data:
        df_buy = pd.DataFrame(item['StockDepthBox']['PriceDepthBox']['BuySide'])
        df_sell = pd.DataFrame(item['StockDepthBox']['PriceDepthBox']['SellSide'])

        order_book = process_order_book(item['Symbol'], item['Term'], df_buy, df_sell)
        df = order_book if df.empty else pd.concat([df, order_book])

    return df

def process_securities(df):

    result_index = ['symbol', 'settlement']
    filter_columns = ['Symbol', 'Term', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate', 'Panel']
    result_columns = ['symbol', 'settlement', 'bid_size', 'bid', 'ask', 'ask_size', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'group']
    numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_size', 'bid', 'ask_size', 'ask', 'previous_close']

    if not df.empty:
        df.TradeDate = pd.to_datetime(df.TradeDate, format='%Y%m%d', errors='coerce') + pd.to_timedelta(df.Hour, errors='coerce')
        df.Term = df.Term.apply(lambda x: __settlements_int[x] if x in __settlements_int else '')
        df.Panel = df.Panel.apply(lambda x: __group_map[x] if x in __group_map else '')

        df = df[filter_columns].copy()
        df.columns = result_columns

        df = convert_to_numeric_columns(df, numeric_columns)
    else:
        df = pd.DataFrame(columns=result_columns)

    df = df.set_index(result_index)
    return df

def process_options(df):

    result_index = ['symbol']
    filter_columns = ['Symbol', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate', 'MaturityDate', 'StrikePrice', 'PutOrCall', 'Issuer']
    result_columns = ['symbol', 'bid_size', 'bid', 'ask', 'ask_size', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'expiration', 'strike', 'kind', 'underlying_asset']
    numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_size', 'bid', 'ask_size', 'ask', 'previous_close', 'strike']

    if not df.empty:
        df.TradeDate = pd.to_datetime(df.TradeDate, format='%Y%m%d', errors='coerce') + pd.to_timedelta(df.Hour, errors='coerce')
        df.MaturityDate = pd.to_datetime(df.MaturityDate, format='%Y%m%d', errors='coerce')
        df.PutOrCall = df.PutOrCall.apply(lambda x: __callput[x] if x in __callput else __callput[0])

        df = df[filter_columns].copy()
        df.columns = result_columns
        df = convert_to_numeric_columns(df, numeric_columns)
    else:
        df = pd.DataFrame(columns=result_columns)

    df = df.set_index(result_index)
    return df[df.strike > 0].copy() # Remove non options rows

def process_repos(df):

    result_index = ['symbol', 'settlement']
    filter_columns = ['Symbol', 'CantDias', 'Term', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate']
    result_columns = ['symbol', 'days', 'settlement', 'bid_amount', 'bid_rate', 'ask_rate', 'ask_amount', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime']
    numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_amount', 'bid_rate', 'ask_rate', 'ask_amount', 'previous_close']

    if not df.empty:
        df.TradeDate = pd.to_datetime(df.TradeDate, format='%Y%m%d', errors='coerce') + pd.to_timedelta(df.Hour, errors='coerce')

        df = df[filter_columns].copy()
        df.columns = result_columns

        df = convert_to_numeric_columns(df, numeric_columns)
    else:
        df = pd.DataFrame(columns=result_columns)

    df = df.set_index(result_index)
    return df

def process_order_book(name, settlement, df_buy, df_sell):

    result_index = ['symbol', 'settlement', 'position']
    filter_buy_columns = ['Pos', 'BuyQuantity', 'BuyPrice', 'NumberOfOrders']
    result_buy_columns = ['position', 'bid_size', 'bid', 'bid_offers_count']
    filter_sell_columns = ['Pos', 'SellQuantity', 'SellPrice', 'NumberOfOrders']
    result_sell_columns = ['position', 'ask_size', 'ask', 'ask_offers_count']

    df = pd.DataFrame(columns=result_index)
    df.position = range(1, 6)
    df.symbol = name
    df.settlement = __settlements_int[settlement] if settlement in __settlements_int else settlement

    df_buy = df_buy[filter_buy_columns] if not df_buy.empty else pd.DataFrame(columns=filter_buy_columns)
    df_buy.columns = result_buy_columns
    df = df.merge(df_buy, on=['position'], how='left')

    df_sell = df_sell[filter_sell_columns] if not df_sell.empty else pd.DataFrame(columns=filter_sell_columns)
    df_sell.columns = result_sell_columns
    df = df.merge(df_sell, on=['position'], how='left')

    df = convert_to_numeric_columns(df, result_buy_columns + result_sell_columns)

    df = df.set_index(result_index)
    return df

##################################
## BOARD & SETTLEMENT FUNCTIONS ##
##################################
def get_board_for_request(board):

    boards = {
        'bluechips': 'accionesLideres',
        'general_board': 'panelGeneral',
        'cedears': 'cedears',
        'government_bonds': 'rentaFija',
        'short_term_government_bonds': 'letes',
        'corporate_bonds': 'obligaciones'}

    if not board.lower() in boards:
        raise DataException('Invalid board name.')

    return boards[board.lower()]

def get_settlement_for_request(settlement_str, symbol=None):

    is_option = symbol and len(symbol) == 10
    if is_option:
        if settlement_str and settlement_str != '':
            raise DataException('Invalid settlement for option.  Settlement for options should be None or empty.')

        return settlement_str or ''

    is_repo = symbol and symbol in ['DOLAR', 'PESOS']
    if is_repo:
        try:
            settlement_date = datetime.strptime(settlement_str, '%Y%m%d')

            return settlement_str
        except ValueError:
            raise DataException('Invalid settlement for repo.  Settlement for repos should be a string with format %Y%m%d (YYYYMMDD)')

    if not settlement_str or not (settlement_str.lower() in __settlements_str):
        raise DataException('Invalid settlement. Settlement for assets should be spot, 24hs or 48hs.')

    return __settlements_str[settlement_str.lower()]

def get_settlement_from_response(settlement_int, symbol=None):

    is_option = symbol and len(symbol) == 10
    is_repo = symbol and symbol in ['DOLAR', 'PESOS']

    if is_option or is_repo:
        return settlement_int

    if not (settlement_int in __settlements_int):
        raise DataException('Invalid settlement. Settlement for assets should be 1, 2 or 3.')

    return __settlements_int[settlement_int]
