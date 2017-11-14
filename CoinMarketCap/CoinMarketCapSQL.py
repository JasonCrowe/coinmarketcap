from sqlalchemy import create_engine
# from sqlalchemy import MetaData, Table, String, Integer, Float, Column, Date
import pandas as pd
from datetime import datetime, timedelta
# from time import strftime
from bs4 import BeautifulSoup
import requests
from time import sleep

engine = create_engine('sqlite:///coinmarketcap.db')
BASE = 'https://coinmarketcap.com'
history_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market_Cap', 'Coin']
ignore_coins = ['mergecoin']


def get_parsed_page(url):
    user_agent = "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6"
    headers = {'User-Agent': user_agent}
    try:
        response = requests.get(url, headers=headers)
        bs = BeautifulSoup(response.content, 'html.parser')
        return bs
    except requests.exceptions.Timeout:
        print('Connection Timeout, trying again...')
        sleep(10)
        response = requests.get(url, headers=headers)
        bs = BeautifulSoup(response.content, 'html.parser')
        return bs
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        print('Connection Error\n', e)
        exit()


def get_coin_list():
    coin_url = BASE + '/all/views/all/'
    bs = get_parsed_page(coin_url)
    coin_table = bs.find('table', attrs={'id': 'currencies-all'})
    links_to_coins = []

    print("Downloading Coin List")
    for row in coin_table.find_all('tr')[1:]:  # Let's skip the headers.
        cells = row.find_all('td')
        link_to_coin = 'https://coinmarketcap.com{}'.format(
            cells[1].find('a').get('href'))
        links_to_coins.append(link_to_coin)
    coins = [x.split('/')[-2:-1][0] for x in links_to_coins]
    coins.sort()
    coins = [c for c in coins if c not in ignore_coins]
    return coins


def get_exchange_data(input_coin):
    exchange_url = 'https://coinmarketcap.com/currencies/{}/#markets'.format(input_coin)
    print exchange_url
    bs = get_parsed_page(exchange_url)

    table = bs.find('table', attrs={'id': 'markets-table'})
    data = list()
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        # print [cells[1].get_text(), input_coin]
        data.append((cells[1].get_text(), input_coin))
    return data


def get_coin_historical_data(input_coin):
    try:
        start_date = coin_last_downloaded_date[input_coin].strftime("%Y%m%d")
    except KeyError:
        start_date = '20010101'
    history_url = BASE + '/currencies/{coin}/historical-data/?start={start_date}&end=21000101'.format(
        coin=input_coin.lower(),
        start_date=start_date
    )
    print history_url

    if start_date != datetime.today().strftime("%Y%m%d"):
        print('Downloading coin historical data: {}'.format(input_coin))
        bs = get_parsed_page(history_url)

        table = bs.find('table', attrs={'class': 'table'})
        coin_history_data = list()
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            data = [c.get_text() for c in cells]
            data.append(input_coin)
            data = clean_scraped_row(data)
            if data is not None:
                coin_history_data.append(data)
            else:
                return None
        return coin_history_data
    else:
        return None


def clean_scraped_row(row):
    print 'Pre-Clean', row
    try:
        row[0] = datetime.strptime(row[0], "%b %d, %Y")
    except ValueError:
        #  No valid values, return None
        print row[0]
        return None

    for r in xrange(7):
        try:
            row[r] = row[r].replace(',', '')
            row[r] = str(row[r].replace('-', ''))
        except TypeError as t:
            print t
            pass
    try:
        row[1] = float(row[1])
        row[2] = float(row[2])
        row[3] = float(row[3])
        row[4] = float(row[4])
    except ValueError:
        return None

    try:
        row[5] = int(row[5].replace('-', ''))
    except ValueError:
        row[5] = None
    try:
        row[6] = int(row[6].replace('-', ''))
    except ValueError:
        row[6] = None
    print 'Post-Clean', row
    return row


def build_last_date_dict(rs):
    d = {rs[0]: rs[1]}
    return d


def build_start_date():
    q = "select Coin, max(Date) as last_downloaded from history group by Coin"
    coin_download_dates = {}
    with engine.connect() as con:
        for r in con.execute(q):
            coin_download_dates[r[0]] = date_2_search(r[1])
    return coin_download_dates


def date_2_search(date_string):
    converted_string = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f')
    return converted_string + timedelta(days=1)


def download_history(input_coins):
    for coin in input_coins:
        coin_history = get_coin_historical_data(coin)
        if coin_history is not None:
            df = pd.DataFrame(coin_history, columns=history_columns)
            df.to_sql('history', engine, if_exists='append', index=False)


def download_exchanges(input_coins):
    exchange_list = [get_exchange_data(coin) for coin in input_coins]
    merged_df = pd.DataFrame(exchange_list)
    merged_df.drop_duplicates(inplace=True)
    # todo fix this
    print merged_df
    # merged_df.to_sql('exchanges', engine, if_exists='replace', index=False)


if __name__ == "__main__":
    all_coins_list = get_coin_list()
    coin_last_downloaded_date = build_start_date()

    if raw_input("Download Exchanges? y/n: ") == 'y':
        download_exchanges(all_coins_list)

    if raw_input("Download History? y/n: ") == 'y':
        download_history(all_coins_list)
