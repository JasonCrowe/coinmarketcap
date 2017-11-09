import pandas as pd
import random
from pprint import pprint
from datetime import datetime
from time import strftime, gmtime
from bs4 import BeautifulSoup
import requests


def get_coin_list():
    session = requests.Session()
    session.headers.update(
        {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'})
    #r = session.get('https://coinmarketcap.com/coins/views/all/')
    r = session.get('https://coinmarketcap.com/all/views/all/')
    bs = BeautifulSoup(r.content, 'html.parser')
    coin_table = bs.find('table', attrs={'id': 'currencies-all'})
    links_to_coins = []
    for row in coin_table.find_all('tr')[1:]:  # Let's skip the headers.
        cells = row.find_all('td')
        link_to_coin = 'https://coinmarketcap.com{}'.format(
            cells[1].find('a').get('href'))
        links_to_coins.append(link_to_coin)
    coins = [x.split('/')[-2:-1] for x in links_to_coins]
    coins = [x[0] for x in coins]
    return coins


def get_coin_historical_data(coin, start_date='20000101', end_date='21000101'):
    history_url = 'https://coinmarketcap.com/currencies/{coin}/historical-data/?start={start_date}&end={end_date}'.format(
        coin=coin.lower(),
        start_date=start_date,
        end_date=end_date
    )

    user_agent_list = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    ]

    num = random.randint(0, (len(user_agent_list) - 1))
    headers = {'User-Agent': user_agent_list[num]}
    # print(history_url)
    print('{}: Downloading coin historical data: {}'.format(strftime("%H:%M:%S", gmtime()), coin))
    try:
        response = requests.get(history_url, headers=headers)
        df_list = pd.read_html(response.content)
        df = df_list[0]
        df['Coin'] = coin
        df['download_date'] = datetime.now().date()
        return df
    except:
        print('Error downloading {}, trying again'.format(coin))
        get_coin_historical_data(coin, start_date, end_date)


def get_coin_exchange_data(coin):
    """
    Build the url from the coin input.
    download the table with pandas and store it into a dataframe
    add the 'Coin' name to the table
    add the 'download_date' to the table
    """
    market_url = 'https://coinmarketcap.com/currencies/{}/#markets'.format(coin.lower())

    user_agent_list = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    ]

    num = random.randint(0, (len(user_agent_list) - 1))
    headers = {'User-Agent': user_agent_list[num]}

    print('{}: Downloading coin exchange data: {}'.format(strftime("%H:%M:%S", gmtime()), coin))
    try:
        response = requests.get(market_url, headers=headers)
        df_list = pd.read_html(response.content)
        df = df_list[0]
        df['Coin'] = coin
        df['download_date'] = datetime.now().date()
        return df
    except:
        print('Error downloading {}, trying again'.format(coin))
        get_coin_exchange_data(coin)


def update_data(coin_list):
    """
    Load all the data for each coin into a single DataFrame for historical and exchange data
    input should be a list of coins.
    - This means you can use 'get_coin_list()' as input or
    - a single coin ['bitcoin'] or
    - a list of coins ['bitcoin', 'ethereum']

    The resulting file and DataFrame will be the input coins

    This uses the python "list comprehension" construct.
    See youtube for more details:
    https://youtu.be/1HlyKKiGg-4
    """
    # coin_list = coin_list[:2]
    historical_df = pd.concat([get_coin_historical_data(x) for x in coin_list])
    # Set the date as a datetime object
    historical_df['Date'] = pd.to_datetime(historical_df['Date'], format='%b %d, %Y')
    exchange_df = pd.concat([get_coin_exchange_data(x) for x in coin_list])
    return historical_df, exchange_df


def save_df_to_filesystem(historical_df, exchange_df):
    """
    save DataFrame to filesystem using a python 'Pickled" object
    this can be changed to a csv:
    - exchange_df.to_csv('exchange_data.csv')
    or an excel file
    - exchange_df.to_excel('exchange_data.xlsx')
    """
    exchange_df.to_pickle('exchange_data.pkl')
    historical_df.to_pickle('historical_data.pkl')


def read_df_from_filesystem():
    """
    read data from pickled object on filesystem into DataFrames and return the
    DataFrames to the filesystem
    """
    exchange_df = pd.read_pickle('exchange_data.pkl')
    historical_df = pd.read_pickle('historical_data.pkl')
    return historical_df, exchange_df


def startup(coin_list):
    """
    If no list is passed to the function, download the current list from
    the website and use that to build the DataFrames
    """
    try:
        historical_df, exchange_df = read_df_from_filesystem()
        """
        Try to read data from filesystem. If it doesn't exist the code will
        throw an IOError which is caught below
        """
    except IOError:
        # update because info is missing
        print('Data not found on filesystem, downloading initial data')
        historical_df, exchange_df = update_data(coin_list)
        print('Data update complete, writing to filesystem')
        save_df_to_filesystem(historical_df, exchange_df)
    """
    Once we have a list we want to check how old it is.
    """
    downloaded_on = historical_df['download_date'].max()
#     print historical_df['download_date']
#     type(historical_df['download_date'])
    """
    Show the user the latest date and let them decide if they want to update the data or not.
    When we started we were dealing with the entire list as a single object, so finding the latest
    date worked fine. We may want to reconsider how this is achomplished now that we are working
    with and downloading specific coins
    """
    if raw_input('Data was last downloaded on {}.\nUpdate Data? y/n: '.format(downloaded_on)) == 'y':
        # download and save if "y" return the saved data if "n"
        print('Starting data update')
        historical_df, exchange_df = update_data(coin_list)
        print('Data update complete, writing to filesystem')
        save_df_to_filesystem(historical_df, exchange_df)
        print('New data has been loaded into DataFrames and saved to filesystem.')
    return historical_df, exchange_df


def shutdown(historical_df, exchange_df):
    """
    As the final step, save data to filesystem this isn't really needed,
    but I want you to know how to save data after analysis
    """
    save_df_to_filesystem(historical_df, exchange_df)


def kelly_list(data):
    if len(data) < 2:
        return []
    kelly_list = [0.0]
    number_positives = number_negatives = pos_delta = neg_delta = average_raise = average_drop = 0.0

    for t in xrange(len(data) - 1):
        old_value = data[t]
        new_value = data[t + 1]
        diff = new_value - old_value

        if diff > 0:
            number_positives += 1.0
            pos_delta += diff
            average_raise = pos_delta / number_positives
        elif diff < 0:
            number_negatives += 1.0
            neg_delta += diff
            average_drop = (-1.0) * (neg_delta / number_negatives)

        if old_value == 0.0:
            kelly_list.append(0.0)
            continue
        """
        diff_in_percentage is not actually used in the program.
        this also means that diff_in_fraction is not used in the program
        """
        #diff_in_fraction = diff / old_value
        #diff_in_percentage = diff_in_fraction * 100.0

        if (number_positives + number_negatives):
            W = number_positives / (number_positives + number_negatives)
        else:
            W = 0

        if average_raise == 0:
            if average_drop == 0:
                K = 0.0
            else:
                K = -1.0
        else:
            if average_drop == 0:
                K = 1.0
            else:
                R = average_raise / average_drop
                K = W - ((1 - W) / R)
        kelly_list.append(K)
    return kelly_list


def calculate_kelly_for_coin(coin, historical_df):
    ###########################################
    # Use KellyIndex code to add to DataFrame #
    ###########################################
    # Sort by date so the kelly index processes in the correct order
    historical_df = historical_df.sort_values(by=['Date'])

    # Store the coin data from the coin into a DataFrame
    coin_historical_data = historical_df[historical_df['Coin'] == coin]
    pd.to_numeric(coin_historical_data['Close'], errors='coerce')
    # Create a list from the Close column of data in the bitcoin DataFrame
    # pass that list to your kellylist function

    kl = kelly_list([x for x in coin_historical_data['Close']])

    # Turn the results into a series which is matched up with the
    #coin_historical_data['kelly_index'] = pd.Series(kl, index=coin_historical_data.index)
    # ^^^ This is not needed if you just want to return the current kelly value.

    # print results
    # print(coin_historical_data)
    # print coin, kl[-1]
    try:
        return kl[-1]
    except IndexError as e:
        print(coin, 'causes and index error. Is there enough data?')
        return 0.0


def calculate_kelly_for_exchange(exchange):
    """
    Notice the following line:
    exchange_df['Source'] == exchange].Coin.unique()]
    We are passing the exchange to the DataFrame as a filter,
    then we select the unique values from the filtered DataFrame.

    It might be a good idea for this function to pass back the results
    rather than just printing them to screen. It is almost always a good
    idea to separate your business logic and presentation logic. It may not
    matter right now, but it could help you a lot in the future.
    """
    print('{line}\n{ex} Coins: \n{line}'.format(ex=exchange, line='*' * 80))
    coins = [x for x in exchange_df[exchange_df['Source'] == exchange].Coin.unique()]
    print(coins)

    for c in coins:
        k = calculate_kelly_for_coin(c, historical_df)
        if k > 0.10:
            print 'Kelly %-25s = %-2.2f %%' % (c, (100*k))

    # print(kraken_exchange_coins)
    # Filter coin/exchange dataframe for 'Kraken' exchange

"""
To add to the idea of separating business logic and presentation logic there is also
merit in the idea of separating out the data layer.

A way in which this could help you is separate the code into the following sections:
1 - Data: this would be a program that runs once a day to update all the data.
2 - Presentation: This isn't needed too much now, but envision a reports that
    are presented to clients.
3 - Business Logic: Much of what we are writing here.
"""
coins_to_track = ['monero', 'melon', 'bitcoin', 'litecoin', 'dash', 'ripple', 'iconomi', 'gnosis-gno', 'stellar']
coins_to_track = get_coin_list()
"""
There is not a good way to save this data back into the main dataframe that we save to disk. Each download is saved as
the working data set. It would be better to store a "working" set with the tracked coins and an "all_records" set that
contains everything. It would be best to store records in a database.
"""

historical_df, exchange_df = startup(coins_to_track)

"""
Cleaning a row is as simple as passing the row to a function
and storing the result in a field
"""
def clean_close(row):
    try:
        c = float(row.Close)
    except ValueError:
        c = 'NA'
    return c
"""
We will use the above function to change the values to a float or change it to NA if
it can't convert to float.
"""
historical_df['cleaned_close'] = historical_df.apply(clean_close, axis=1)
"""
After the data is converted, we can limit the selection to only float values
"""
# print historical_df[historical_df['cleaned_close'] != 'NA']

"""
Pandas also has built in ways to insure the data is the correct type
The errors='coerce' tells the function to turn the values into a NaN object
NaN stands for Not a Number. This is probably a better direction, but I am not real
knowledgeable on NaN and how to filter them.

# pd.to_numeric(historical_df['Close'], errors='coerce')
"""

all_exchanges = [calculate_kelly_for_exchange(x) for x in exchange_df.Source.unique()]
# CalculateKellyForExchange('bittrex')
# CalculateKellyForExchange('Kraken')

kelly_percentage = dict()
kelly_percentage[u'XXMR'] = 100 * calculate_kelly_for_coin('monero', historical_df)
kelly_percentage[u'XMLN'] = 100 * calculate_kelly_for_coin('melon', historical_df)
kelly_percentage[u'XXBT'] = 100 * calculate_kelly_for_coin('bitcoin', historical_df)
kelly_percentage[u'XLTC'] = 100 * calculate_kelly_for_coin('litecoin', historical_df)
kelly_percentage[u'DASH'] = 100 * calculate_kelly_for_coin('dash', historical_df)
kelly_percentage[u'XXRP'] = 100 * calculate_kelly_for_coin('ripple', historical_df)
kelly_percentage[u'XICN'] = 100 * calculate_kelly_for_coin('iconomi', historical_df)
kelly_percentage[u'GNO'] = 100 * calculate_kelly_for_coin('gnosis-gno', historical_df)
kelly_percentage[u'XXLM'] = 100 * calculate_kelly_for_coin('stellar', historical_df)

print("Kelly Percentage")
pprint(kelly_percentage)

"""
I think you should separate the data and the code. Writing a dictionary to an external file
is not really the best idea for python. Unfortunately, I cannot give you a great reason why,
but I know that it not done like this for other python programs.

Persistent data is really best stored in a database or even a CSV file. Those can be easily
read and written by pandas
"""
to_write = "Kelly Percentage=%s" % kelly_percentage
file_object = open('KellyValues.py', 'w')
file_object.write(to_write)
file_object.close()
