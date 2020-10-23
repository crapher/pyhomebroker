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

import datetime
from pyhomebroker import HomeBroker

broker = input('Numero de Agente: ')
dni = input('DNI: ')
user = input('Usuario: ')
password = input('Clave: ')

hb = HomeBroker(int(broker))
hb.auth.login(dni=dni, user=user, password=password, raise_exception=True)

data = hb.history.get_daily_history('GGAL', datetime.date(2015, 1, 1), datetime.date(2020, 1, 1))
print(data)