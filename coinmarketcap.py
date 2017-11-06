import pandas as pd
# pandas library is everything you should need to analyse data
# see youtube: 
# - https://youtu.be/-NR-ynQg0YM
# youtube for stock market analysis: 
# - https://www.youtube.com/results?search_query=pandas+stock+market+data
from datetime import datetime
# to check the date and see if info is old
from time import strftime, gmtime
# to check the date and see if info is old
from bs4 import BeautifulSoup
import requests
# bs4 and request to get the url of all the coins


def get_coin_list():
    # basicly the same code from your last program
    session = requests.Session()
    session.headers.update({'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'})
    r = session.get('https://coinmarketcap.com/coins/views/all/')
    bs = BeautifulSoup(r.content, 'html.parser')        
    coin_table = bs.find('table', attrs={'id': 'currencies-all'})
    #  Got it!  Now, we're going to search for all that table's tr HTML
    #  tags.  tr stand for "table row", so let's call them rows.
    links_to_coins = []
    #  This is where we'll store the scraped links to the coins.
    for row in coin_table.find_all('tr')[1:]:  # Let's skip the headers.
        cells = row.find_all('td')
        #  Table rows contain td tags, which stands for table data.
        #  This represents the column-delimited information on the site.
        link_to_coin = 'https://coinmarketcap.com' + \
                       cells[1].find('a').get('href')
        #  We find the link to the coin's data by going to the first
        #  tag, locating the link tag, and retrieving the link itself,
        #  AKA 'href' - remember, list indexes in Python start with 0!
        links_to_coins.append(link_to_coin)
        #  We add the scraped link to the links_to_coins list,
        #  and then the 'for' loop continues onto the next row.
        coins = [x.split('/')[-2:-1] for x in links_to_coins]
        # We split the url into a list to get the specific coin name
        coins = [x[0] for x in coins]
        # combine conbime the list of list into a list of strings
    return coins


def get_coin_historical_data(coin):
    """
    Build the url from the coin input
    download the table with pandas and store it into a dataframe
    add the 'Coin' name to the table
    add the 'download_date' to the table
    """
    history_url = 'https://coinmarketcap.com/currencies/{}/historical-data/?start=20000101&end=21000101'.format(coin.lower())
    print('{}: Downloading coin historical data: {}'.format(strftime("%H:%M:%S", gmtime()), coin))
    df = pd.read_html(history_url)[0]
    df['Coin'] = coin
    df['download_date'] = datetime.now().date()
    return df
  
    
def get_coin_exchange_data(coin):
    """
    Build the url from the coin input.
    download the table with pandas and store it into a dataframe
    add the 'Coin' name to the table
    add the 'download_date' to the table
    """
    market_url = 'https://coinmarketcap.com/currencies/{}/#markets'.format(coin.lower())
    print('{}: Downloading coin exchange data: {}'.format(strftime("%H:%M:%S", gmtime()), coin))
    df = pd.read_html(market_url)
    df = df[0]
    df['Coin'] = coin
    df['download_date'] = datetime.now().date()
    return df


def update_data(coin_list):
    """
    Load all the data for each coin into a single dataframe for historical and exchange data
    input should be a list of coins. 
    - This means you can use 'get_coin_list()' as input or 
    - a single coin ['bitcoin'] or 
    - a list of coins ['bitcoin', 'ethereum']
    
    The resulting file and dataframe will be the input coins
    
    This uses the python "list comprehension" construct.
    See youtube for more details:
    https://youtu.be/1HlyKKiGg-4
    """
#     coins = coins[:2]
    historical_df = pd.concat([get_coin_historical_data(x) for x in coins])
    exchange_df = pd.concat([get_coin_exchange_data(x) for x in coins])
    return historical_df, exchange_df


def save_df_to_filesystem(historical_df, exchange_df):
    """
    save dataframe to filesystem using a python 'Pickled" object
    this can be changed to a csv:
    - exchange_df.to_csv('exchange_data.csv')
    or an excel file
    - exchange_df.to_excel('exchange_data.xlsx')
    """
    exchange_df.to_pickle('exchange_data.pkl')
    historical_df.to_pickle('historical_data.pkl')
    
    
def read_df_from_filesystem():
    """
    read data from pickled object on filesystem into dataframes and return the
    dataframes to the filesystem
    """
    exchange_df = pd.read_pickle('exchange_data.pkl')
    historical_df = pd.read_pickle('historical_data.pkl')
    return historical_df, exchange_df


def startup():
    try:
        historical_df, exchange_df = read_df_from_filesystem()
        # try to read data from filesystem. If it is missing go to next step
    except IOError:
        # update because info is missing
        print('Data not found on filesystem, downloading initial data')
        historical_df, exchange_df = update_data(get_coin_list())
        print('Data update complete, writing to filesystem')
        save_df_to_filesystem(historical_df, exchange_df)
    finally:
        # after we either download or pull from file we need to find the downloaded date
        downloaded_on = historical_df['download_date'].max()

    if raw_input('Data was last downloaded on {}.\nUpdate Data? y/n: '.format(downloaded_on)) == 'y':
        # download and save if "y"
        # return the saved data if "n"
        print('Starting data update')
        historical_df, exchange_df = update_data(get_coin_list())
        print('Data update complete, writing to filesystem')
        save_df_to_filesystem(historical_df, exchange_df)
        print('New data has been loaded into DataFrames and saved to filesystem.')
    return historical_df, exchange_df

def shutdown(historical_df, exchange_df):
    # As the final step, save data to filesystem
    # this isn't really needed, but I want you to know how to save data after analysis
    save_df_to_filesystem(historical_df, exchange_df)

    
#######################################
# Seperation of functions and program #
#######################################


historical_df, exchange_df = startup()
# load the data into dataframes (either from filesystem or download)

# Filter coin historical dataframe for 'bitcoin' coin
bitcoin_historical_data = historical_df[historical_df['Coin'] == 'bitcoin']
print 'print bitcoin_historical_data'
print bitcoin_historical_data

# Filter coin/exchange dataframe for 'bitfinex' exchange
bitfinex_exchange_coins = exchange_df[exchange_df['Source'] == 'bitfinex']
print 'print bitfinex_exchange_coins'
print bitfinex_exchange_coins

# Filter coin/exchange dataframe for 'bitcoin' coin
bitcoin_exchanges = exchange_df[exchange_df['Coin'] == 'bitcoin']
print 'print bitcoin_exchanges'
print bitcoin_exchanges

shutdown(historical_df, exchange_df)
# final step saves data to filesystem. Not needed if you don't change the actual dataframe data or structure
