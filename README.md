# coinmarketcap
Files & what they do...

## coinmarketcap.db
SQLite file holding data from coinmarketcap.com.
### Tables
* history - historic trading data
* exchanges - list of exchanges and the coins traded in them

## UpdateCoinMartketCapData.py
This file updates the historical data by adding only the updates.
This file updates the exchange data by replacing the table with updated results at each run

## coins.py
coins.py will work with the data in the database. 
