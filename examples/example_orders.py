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

from pyhomebroker import HomeBroker

broker = input('Numero de Agente: ')
dni = input('DNI: ')
user = input('Usuario: ')
password = input('Clave: ')
account_id = input('Comitente: ')

hb = HomeBroker(int(broker))
hb.auth.login(dni=dni, user=user, password=password, raise_exception=True)

orders = hb.orders.get_orders_status(account_id)
print(orders)

## Send a buy order to the market
#symbol = input('Simbolo: ')
#settlement = input('Vencimiento: ')
#price = input('Precio: ')
#size = input('Cantidad: ')
#order_number = hb.orders.send_buy_order(symbol, settlement, float(price), int(size))
#print(order_number)

## Send a sell order to the market
#symbol = input('Simbolo: ')
#settlement = input('Vencimiento: ')
#price = input('Precio: ')
#size = input('Cantidad: ')
#order_number = hb.orders.send_sell_order(symbol, settlement, float(price), int(size))
#print(order_number)

## Cancel an order
#order_number = input('Order number: ')
#hb.orders.cancel_order(account_id, order_number)

## Cancel all the orders
#hb.orders.cancel_all_orders(account_id)
