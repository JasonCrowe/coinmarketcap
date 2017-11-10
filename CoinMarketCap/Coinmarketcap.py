import pandas as pd
import random
from datetime import datetime, timedelta
from time import strftime, gmtime
from bs4 import BeautifulSoup
import requests
import os.path

SUB = 'data'

try:
    os.mkdir(SUB)
except:
    pass


class CoinMarketCapScraper:
    def __init__(self):
        self.user_agent_list = [
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        ]
        # self.coin_list = None
        # self.coin_ignore = []

    def scrape_coin_list(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6'
        })
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
        # self.coin_list = [c for c in coins if c not in self.coin_ignore]
        return coins

    def get_coin_historical_data(self, coin, start_date='20000101', end_date='21000101'):
        # print 'trying to download the following data', start_date, end_date
        history_url = 'https://coinmarketcap.com/currencies/{coin}/historical-data/?start={start_date}&end={end_date}'.format(
            coin=coin.lower(),
            start_date=start_date,
            end_date=end_date
        )
        # print history_url
        num = random.randint(0, (len(self.user_agent_list) - 1))
        headers = {'User-Agent': self.user_agent_list[num]}
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
            print(history_url)
            self.get_coin_historical_data(coin, start_date, end_date)

    def get_coin_exchange_data(self, coin):
        market_url = 'https://coinmarketcap.com/currencies/{}/#markets'.format(coin.lower())
        num = random.randint(0, (len(self.user_agent_list) - 1))
        headers = {'User-Agent': self.user_agent_list[num]}
        print('{}: Downloading coin exchange data: {}'.format(strftime("%H:%M:%S", gmtime()), coin))
        try:
            response = requests.get(market_url, headers=headers)
            try:
                df_list = pd.read_html(response.content)
                df = df_list[0]
                df['Coin'] = coin
                df['download_date'] = datetime.now().date()
            except ValueError:
                print('Page missing information, returning empty dataframe')
                df = pd.DataFrame()
            return df
        except:  # TODO add extra error checking for specific types of downloading errors
            print('Error downloading {}, trying again'.format(coin))
            print market_url
            self.get_coin_exchange_data(coin)


class Coin:
    def __init__(self, coin_name):
        self.coin_name = coin_name
        self.history = None
        self.exchange = None
        self.kelly_index_value = None
        self.downloaded_on = None

        try:
            self.unpickle_history()
            print('unpickled history: {}'.format(self.coin_name))
        except IOError:
            self.history = self.download_history()
            print('File not found, downloading history: {}'.format(self.coin_name))
            self.pickle_history()
            print('pickled history: {}'.format(self.coin_name))

        try:
            self.downloaded_on = self.history['download_date'].max()
            if self.downloaded_on != datetime.now().date():
                self.history = pd.concat([self.history, self.download_partial_history()])
                self.history['download_date'] = datetime.now().date()
                self.pickle_history()
        except (IOError, KeyError):
            self.download_history()
            self.pickle_history()

        try:
            self.unpickle_exchange()
        except IOError:
            self.download_exchange()
            self.pickle_exchange()

        try:
            self.downloaded_on = self.exchange['download_date'].max()
            if self.downloaded_on != datetime.now().date():
                self.download_exchange()
                self.pickle_exchange()
        except KeyError:
            print('Manual Check: https://coinmarketcap.com/currencies/{}/historical-data/?start=20000101&end=22222222'.format(self.coin_name))

    def __repr__(self):
        return '< name : {}, kelly_index: {} >'.format(self.coin_name, self.kelly_index)

    def download_partial_history(self):
            return self.download_history((self.downloaded_on - timedelta(days=1)).strftime('%Y%m%d'))

    def kelly_index(self):
        data = self.history.sort_values(by=['Date'])
        data = data['Close']

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
        kl = kelly_list[-1:]
        self.kelly_index_value = kl[0] * 100
        return kl[0] * 100

    def download_exchange(self):
        cmcs = CoinMarketCapScraper()
        self.exchange = cmcs.get_coin_exchange_data(self.coin_name)

    def download_history(self, start_date='20010101'):
        cmcs = CoinMarketCapScraper()
        try:
            downloaded_history = cmcs.get_coin_historical_data(self.coin_name, start_date=start_date)
            downloaded_history['Date'] = pd.to_datetime(downloaded_history['Date'], format='%b %d, %Y')
            return downloaded_history
        except ValueError as e:
            print(e)
            self.download_history()

    def unpickle_history(self):
        self.history = pd.read_pickle(os.path.join(SUB, '{}_history.pkl'.format(self.coin_name)))

    def unpickle_exchange(self):
        self.exchange = pd.read_pickle(os.path.join(SUB, '{}_exchange.pkl'.format(self.coin_name)))

    def pickle_history(self):
        self.history.to_pickle(os.path.join(SUB, '{}_history.pkl'.format(self.coin_name)))

    def pickle_exchange(self):
        self.exchange.to_pickle(os.path.join(SUB, '{}_exchange.pkl'.format(self.coin_name)))


def main():
    cmc = CoinMarketCapScraper()
    coin_list = cmc.scrape_coin_list()
    coin_list.sort()

    print coin_list
    # update all coin_data
    for c in coin_list:
        Coin(c)

if __name__ == '__main__':
    main()

    # # work with individual coin
    # centra = Coin('centra')
    # print centra.history