Change Log
==========

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