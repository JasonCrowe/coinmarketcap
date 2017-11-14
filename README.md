# coinmarketcap
## URLS
http://coinmarketcap.com
Files & what they do...

## coinmarketcap.db
SQLite file holding data from coinmarketcap.com.
### Tables
* history - https://coinmarketcap.com/currencies/bitcoin/historical-data/?start=20130428&end=20171114
** historic trading data

* exchanges - https://coinmarketcap.com/currencies/bitcoin/#markets
** list of exchanges and the coins traded in them


## UpdateCoinMartketCapData.py
This file updates the historical data by adding only the updates.
This file updates the exchange data by replacing the table with updated results at each run

## coins.py
coins.py will work with the data in the database. 
