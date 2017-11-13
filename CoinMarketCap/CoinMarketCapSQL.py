from sqlalchemy import create_engine
# from sqlalchemy import MetaData, Table, String, Integer, Float, Column, Date
import pandas as pd
from datetime import datetime, timedelta
from time import strftime, gmtime
from bs4 import BeautifulSoup
import requests
from time import sleep

engine = create_engine('sqlite:///coinmarketcap.db')
BASE = 'https://coinmarketcap.com'
history_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market_Cap', 'Coin']


def get_parsed_page(url):
    user_agent = "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6"
    headers = {'User-Agent': user_agent}
    try:
        response = requests.get(url, headers=headers)
        bs = BeautifulSoup(response.content, 'html.parser')
        return bs
    except requests.exceptions.Timeout:
        print('Connection Timeout')
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
    for row in coin_table.find_all('tr')[1:]:  # Let's skip the headers.
        cells = row.find_all('td')
        link_to_coin = 'https://coinmarketcap.com{}'.format(
            cells[1].find('a').get('href'))
        links_to_coins.append(link_to_coin)
    coins = [x.split('/')[-2:-1] for x in links_to_coins]
    coins = [x[0] for x in coins]
    coins.sort()
    return coins


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
        print('{}: Downloading coin historical data: {}'.format(strftime("%H:%M:%S", gmtime()), input_coin))
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
        print row[0]
        return None

    for r in xrange(7):
        try:
            row[r] = row[r].replace(',', '')
            row[r] = str(row[r].replace('-', ''))
        except TypeError:
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
    try:
        q = "select Coin, max(Date) as last_downloaded from history group by Coin"
        coin_download_dates = {}
        with engine.connect() as con:
            for r in con.execute(q):
                coin_download_dates[r[0]] = date_2_search(r[1])
        return coin_download_dates
    except:
        return dict()


def date_2_search(date_string):
    converted_string = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f')
    return converted_string + timedelta(days=1)


all_coins_list = get_coin_list()
coin_last_downloaded_date = build_start_date()

for coin in all_coins_list:
    coin_history = get_coin_historical_data(coin)
    if coin_history is not None:
        df = pd.DataFrame(coin_history, columns=history_columns)
        print df
        df.to_sql('history', engine, if_exists='append', index=False)