Change Log
==========

0.54
---
- Added Veta Capital S.A. to brokers list

0.53
---
- Fix bug on gathering information from repos

0.52
---
- Added SolFin to brokers list

0.51
---
- Added total amount to the order df

0.50
---
- Fix missing parameters sending validation orders

0.49
---
- Fix SessionException reference
- Add close price to boards in the market snapshot.

0.48.1
---
Fix problem with login on homebroker version 1.12.1

0.48
---
- Add alternative login method when the main login fails

0.47
---
- Fix bug with order cancelation

0.46
---
- Fix problem where data is None in the signalR events.

0.45
---
- Add "Negocios Financieros y Bursátiles S.A. (Cocos Capital)" to supported brokers

0.44
---
- Change PSEC URL because they started to use HTTP instead of HTTPS
- Change PSEC broker id to the correct one

0.43
---
- Allow to download a market snapshot

0.42
---
- Fix error when the operation requests a reconfirmation
- Fix error when the system tries to cancel an operation in a currency that is not in AR$

0.41
---
- Remove non needed methods from online module
- Fix error on order status when the price has decimal number
- Add the ability to send buy/sell orders to the market
- Add the ability to cancel an order or all orders
- Add "Sailing S.A." to supported brokers

0.4
---
- Sort code in folders based on functionality
- Add orders module to get status of orders

0.3
---
- Decouple data acquisition from data processing to avoid a bottleneck that caused delays in the quotes notification
- Minor performance improvements when empty data was received

0.2
---
- Add Historical Intraday data
- Add "Alfy Inversiones S.A." to supported brokers
- Add group name to securities to identify where the security is coming from
- Add "Servente y Cia. S.A." to supported brokers
- Fix problem with settlement detection on order book
- Add a method to know if the SignalR server is connected
- Add error handling on the events triggered when the information comes from the online_scrapping module
- **WARNING**: Change bidsize to bid_size and asksize to ask_size for consistency
- **WARNING**: Add connection_lost parameter to on_error event to know if the connection needs to be reestablished.
- **WARNING**: Add order_book_quotes parameter to on_personal_portfolio event to have the order book information available for all the assets in the personal portfolio

0.1.1
-----
- Add "Bolsa de Comercio del Chaco" to supported brokers
- Add "Bell Investments S.A." to supported brokers
- Add Underlying asset to options and personal portfolio
- Fix "Caller is not authorized to invoke the JoinGroup method on StockPricesHub." problem

0.1
---
- Initial revision (Alpha)