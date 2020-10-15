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

def example_online():

    broker = input('Numero de Agente: ')
    dni = input('DNI: ')
    user = input('Usuario: ')
    password = input('Clave: ')

    hb = HomeBroker(int(broker), 
        on_open=on_open, 
        on_personal_portfolio=on_personal_portfolio, 
        on_securities=on_securities, 
        on_options=on_options, 
        on_repos=on_repos, 
        on_order_book=on_order_book, 
        on_error=on_error, 
        on_close=on_close)
        
    hb.auth.login(dni=dni, user=user, password=password, raise_exception=True)
    
    hb.online.connect()
    hb.online.subscribe_personal_portfolio()
    hb.online.subscribe_securities('bluechips','48hs')
    hb.online.subscribe_options()
    hb.online.subscribe_repos()
    hb.online.subscribe_order_book('GGAL', '48hs')
    
    input('Press Enter to Disconnect...\n')

    hb.online.unsubscribe_order_book('GGAL', '48hs')
    hb.online.unsubscribe_repos()
    hb.online.unsubscribe_options()
    hb.online.unsubscribe_securities('bluechips','48hs')
    hb.online.unsubscribe_personal_portfolio()

    hb.online.disconnect()

def on_open(online):
    
    print('=================== CONNECTION OPENED ====================')

def on_personal_portfolio(online, quotes):
    
    print('------------------- Personal Portfolio -------------------')
    print(quotes)

def on_securities(online, quotes):
    
    print('----------------------- Securities -----------------------')
    print(quotes)

def on_options(online, quotes):
    
    print('------------------------ Options -------------------------')
    print(quotes)

def on_repos(online, quotes):
    
    print('------------------------- Repos --------------------------')
    print(quotes)

def on_order_book(online, quotes):
    
    print('------------------ Order Book (Level 2) ------------------')
    print(quotes)
    
def on_error(online, error):
    
    print('@@@@@@@@@@@@@@@@@@@@@@@@@ Error @@@@@@@@@@@@@@@@@@@@@@@@@@')
    print(error)

def on_close(online):

    print('=================== CONNECTION CLOSED ====================')

if __name__ == '__main__':
    example_online()