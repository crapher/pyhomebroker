# Home Broker® API - Market and historical data downloader

[![PyPI pyversions](https://img.shields.io/badge/python-3.6+-blue.svg?style=flat
)](https://pypi.org/project/pyhomebroker) [![PyPI version shields.io](https://img.shields.io/pypi/v/pyhomebroker.svg?maxAge=60)](https://pypi.org/project/pyhomebroker) [![PyPI status](https://img.shields.io/pypi/status/pyhomebroker.svg?maxAge=60)](https://pypi.org/project/pyhomebroker) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)  [![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://paypal.me/ddegese) [![Tweeting](https://img.shields.io/twitter/follow/diegodegese.svg?style=social&label=Follow&maxAge=60)](https://twitter.com/diegodegese)

## Overview
pyhomebroker is an API to connect any python program to the market to receive quotes information in real-time.  Also, it allows for downloading historical data from the home broker platform.

**It requires an account on one of the [supported brokers](#supported-brokers).**

## Quick Start

Pyhomebroker has three modules, the online module, the history module, and the orders module.

### Online Module

The online module handles the connection and subscription with the server and allows a client to subscribe to the home broker platform to receive all the events changes.

    from pyhomebroker import HomeBroker
    
    hb = HomeBroker(
        # Broker ByMA id
        81, 
        # Event triggered when the connection is open
        on_open=on_open_callback, 
        # Event triggered when a new quote is received from the personal portfolio
        on_personal_portfolio=on_personal_portfolio_callback, 
        # Event triggered when a new quote is received from any of the supported security boards
        on_securities=on_securities_callback, 
        # Event triggered when a new quote is received from the options board
        on_options=on_options_callback, 
        # Event triggered when a new quote is received from the repos board
        on_repos=on_repos_callback, 
        # Event triggered when a new quote is received from the order book (level 2)
        on_order_book=on_order_book_callback, 
        # Event triggered when there is an error with the connection
        on_error=on_error_callback
        # Event triggered when the connection is closed
        on_close=on_close_callback)

    # Authenticate with the homebroker platform
    hb.auth.login(dni='12345678', user='user', password='password', raise_exception=True)
    
    # Connect to the server
    hb.online.connect()
    
    # Subscribe to personal porfolio
    hb.online.subscribe_personal_portfolio()
    
    # Subscribe to security board (bluechips, general_board, cedears, government_bonds, short_term_government_bonds, corporate_bonds)
    hb.online.subscribe_securities('bluechips','48hs')
    
    # Subscribe to options board
    hb.online.subscribe_options()
    
    # Subscribe to repos board
    hb.online.subscribe_repos()
    
    # Subscribe to order book of an specific asset
    hb.online.subscribe_order_book('GGAL', '48hs')
    
    # Unsubscribe from the order book of an specific asset
    hb.online.unsubscribe_order_book('GGAL', '48hs')
    
    # Unsubscribe from repos board
    hb.online.unsubscribe_repos()
    
    # Unsubscribe to options board
    hb.online.unsubscribe_options()
    
    # Unsubscribe from a security board (bluechips, general_board, cedears, government_bonds, short_term_government_bonds, corporate_bonds)
    hb.online.unsubscribe_securities('bluechips','48hs')
    
    # Unsubscribe from personal porfolio
    hb.online.unsubscribe_personal_portfolio()
    
    # Disconnect from the server
    hb.online.disconnect()
    
    # Callback signature for on_open event
    def on_open_callback(online):
        pass
    
    # Callback signature for on_personal_portfolio event
    def on_personal_portfolio(online, portfolio_quotes, order_book_quotes):
        pass
    
    # Callback signature for on_securities event
    def on_securities_callback(online, quotes):
        pass
    
    # Callback signature for on_options event
    def on_options_callback(online, quotes):
        pass
    
    # Callback signature for on_repos event
    def on_repos_callback(online, quotes):
        pass
    
    # Callback signature for on_order_book event
    def on_order_book_callback(online, quotes):
        pass
    
    # Callback signature for on_error event
    def on_error_callback(online, exception, connection_lost):
        pass
    
    # Callback signature for on_close event
    def on_close_callback(online):
        pass

The file **[example_online.py](https://github.com/crapher/pyhomebroker/blob/master/examples/example_online.py)** shows a complete working out of the box example.

### History Module

The history module is used to download historical daily data and historical intraday data (only for the current day).

Historical daily data example:

    from pyhomebroker import HomeBroker

    hb = HomeBroker(
        # Broker ByMA id
        81)

    # Authenticate with the homebroker platform
    hb.auth.login(dni='12345678', user='user', password='password', raise_exception=True)

    # Get daily information from platform 
    data = hb.history.get_daily_history('GGAL', datetime.date(2015, 1, 1), datetime.date(2020, 1, 1))

The file **[example_daily_history.py](https://github.com/crapher/pyhomebroker/blob/master/examples/example_daily_history.py)** shows a complete working out of the box example.

Historical intraday data example:

    from pyhomebroker import HomeBroker

    hb = HomeBroker(
        # Broker ByMA id
        81)

    # Authenticate with the homebroker platform
    hb.auth.login(dni='12345678', user='user', password='password', raise_exception=True)

    # Get intraday information from platform
    data = hb.history.get_intraday_history('PAMP')

The file **[example_intraday_history.py](https://github.com/crapher/pyhomebroker/blob/master/examples/example_intraday_history.py)** shows a complete working out of the box example.

### Orders Module

The orders module is used to get information about the status of the orders operated during the day, send sell and buy orders to the market, and cancel one or all the pending orders.

    from pyhomebroker import HomeBroker

    hb = HomeBroker(
        # Broker ByMA id
        81)

    # Authenticate with the homebroker platform
    hb.auth.login(dni='12345678', user='user', password='password', raise_exception=True)

    # Get orders status for account 14565
    orders = hb.orders.get_orders_status(14565)
    print(orders)

	# Send a sell order to the market (Symbol: GGAL - Settlement: 48hs - Price: 120 - Size: 1)
	order_number = hb.orders.send_sell_order('GGAL', '48hs', 120, 1)
	print(order_number)

	# Send a buy order to the market (Symbol: AAPL - Settlement: 48hs - Price: 1900 - Size: 1)
	order_number = hb.orders.send_buy_order('AAPL', '24hs', 1900, 1)
	print(order_number)
	
	# Cancel the order 1690496 for the account 14565
	hb.orders.cancel_order(14565, 1690496)
	
	# Cancel all the orders for the account 14565
	hb.orders.cancel_all_orders(14565)
	
The file **[example_orders.py](https://github.com/crapher/pyhomebroker/blob/master/examples/example_orders.py)** shows a complete working out of the box example.  
All the operations that may affect the broker account (sell, buy, and cancel orders) are commented in the example.

:warning: | The methods to send and cancel orders are still in alpha stage and <u>may have errors</u>.
:---: | :---
:exclamation: | It is **NOT** recommended to use them if you don't know enough about the market or you don't know python deeply.  If after this warning you decide to use them, welcome aboard and ***use them at your own risk***.

## Supported Brokers

| Broker|Byma Id|
| ------------ | :------------: |
|Buenos Aires Valores S.A.|12|
|Proficio Investment S.A.|20|
|Tomar Inversiones S.A.|81|
|Bell Investments S.A.|88|
|Maestro y Huerres S.A.|127|
|Bolsa de Comercio del Chaco|153|
|Prosecurities S.A.|164|
|Servente y Cia. S.A.|186|
|Alfy Inversiones S.A.|201|
|Invertir en Bolsa S.A.|203|
|Futuro Bursátil S.A.|209|
|Sailing S.A.|233|
|Negocios Financieros y Bursátiles S.A. (Cocos Capital)|265|

## Known Issues

One of the dependencies (signalr-client-threads) does not have proxy support when it uses websockets for connection.  I already sent a pull-request to add proxy support to the library and it was approved by the owner.  Now, I have to wait until he merges the changes to the version uploaded to PyPI.

## Installation

Install pyhomebroker from PyPI:

    $ pip install pyhomebroker --upgrade --no-cache-dir

Install development version of pyhomebroker from github:

    $ pip install git+https://github.com/crapher/pyhomebroker --upgrade --no-cache-dir

## Requirements

* [Python](https://www.python.org) >= 3.6+
* [Pandas](https://github.com/pydata/pandas) >= 1.0.0
* [Numpy](http://www.numpy.org) >= 1.18.1
* [Requests](http://docs.python-requests.org/en/master) >= 2.21.0
* [Signalr Client Threads](https://github.com/PawelTroka/signalr-client-threads) >= 0.0.12
* [PyQuery](https://pythonhosted.org/pyquery) >= 1.2

## Legal

See the file [LICENSE](https://github.com/crapher/pyhomebroker/blob/master/LICENSE) for our legal disclaimers of responsibility, fitness, or merchantability of this library as well as your rights with regards to the use of this library.  **pyhomebroker** is licensed under **Apache Software License**.

## Attributions and Trademarks

Home Broker is trademark of Estudio Gallo S.R.L.