import csv

from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import numpy as np


def LimitCoinMarketCap(data, close):
    for t in xrange(len(data)):
        if data[t] == "-":
            return t - 1
    return t - 1


def PrintListInColumn(dd):
    for d in dd:
        print "%3.4f" % d


def PrintListPercentageInColumn(dd):
    for d in dd:
        print "%3.2f%% \t %3.2f \t %d" % ((d[0] * 100), d[1], d[2])


def KellyFloat(data):
    if len(data) < 2: return 0
    difference = np.diff(data)
    # difference=[next-current for current, next in zip(data, data[1:])]
    DifferenceFraction = [delta / datapoint for datapoint, delta in zip(data[:-1], difference)]
    DifferencePercentage = [p * 100 for p in DifferenceFraction]
    pos = neg = posDelta = negDelta = 0.0
    for t in DifferencePercentage:
        if t > 0:
            pos += 1
            posDelta += t
        elif t < 0:
            neg += 1
            negDelta += t
    W = pos / (pos + neg)
    averageGain = posDelta / pos
    averageLost = (-1.0) * (negDelta / neg)
    R = averageGain / averageLost
    KellyFloat = W - ((1 - W) / R)
    return KellyFloat


def KellyList(data):
    if len(data) < 2: return []
    KellyList = [0.0]  # difference=[];DifferenceFraction=[];DifferencePercentage=[]
    NumberPositives = NumberNegatives = posDelta = negDelta = AverageRaise = AverageDrop = 0.0

    for t in xrange(len(data) - 1):
        oldValue = data[t]
        newValue = data[t + 1]
        diff = newValue - oldValue
        # print t, oldValue,newValue,diff

        if diff > 0:
            NumberPositives += 1.0
            posDelta += diff
            AverageRaise = posDelta / NumberPositives
            # print t,posDelta,NumberPositives,AverageRaise
        elif diff < 0:
            NumberNegatives += 1.0
            negDelta += diff
            AverageDrop = (-1.0) * (negDelta / NumberNegatives)
            # print t,negDelta,NumberNegatives,AverageDrop

        if oldValue == 0.0:
            KellyList.append(0.0)
            continue
        DiffInFraction = diff / oldValue
        DiffInPercentage = DiffInFraction * 100.0  # difference.             append(diff);DifferenceFraction.     append(DiffInFraction);DifferencePercentage.   append(DiffInPercentage)
        W = NumberPositives / (NumberPositives + NumberNegatives)

        try:
            R = AverageRaise / AverageDrop
            K = W - ((1 - W) / R)
        except ZeroDivisionError:
            K = 1.0
        # print t,NumberPositives,NumberNegatives,W,R,K
        KellyList.append(K)
    return KellyList


class Coin:
    """
    Contains data for a day's results, and a method to write the data to a
    file. _'s in date_ and open_ are because date and open are builtin
    Python objects, and new variables should not use builtin's names.
    """

    def __init__(self, name):
        self.name = name
        self.date_ = []
        self.open_ = []
        self.high = []
        self.low = []
        self.close = []
        self.volume = []
        self.market_cap = []
        self.exchanges = []

    def writer(self):
        """
        Writes to the CSV file we created with file_handler().
        :return: None
        """
        with open('coinmarketcap_results.csv', 'a') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                [self.name, self.date_, self.open_, self.high, self.low,
                 self.close, self.volume, self.market_cap, self.exchanges]
            )


def create_http_session():
    """
    Quick little function for returning a requests.Session() instance
    with a properly set User-Agent header.
    :return: requests.Session()
    """
    session = requests.Session()
    session.headers.update({'User-Agent':
                                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
                            })
    return session


def soupify(session, url):
    """
    Makes parse-able HTML from any given URL.
    :param session: requests.Session()
    :param url: str
    :return: BeautifulSoup object
    """
    r = session.get(url)
    return BeautifulSoup(r.content, 'html.parser')


class CoinMarketCap_Scraper:
    """
    Handles the logic for the web-scraping of
    https://coinmarketcap.com/coins/views/all/.
    """

    def __init__(self):
        self.session = create_http_session()

    def scrape_coin_links(self):
        """
        Scrapes https://coinmarketcap.com/coins/views/all/ for
        valid links to every coin listed on the website.
        :return: list
        """
        soup = soupify(
            self.session, 'https://coinmarketcap.com/coins/views/all/')
        #  Time to ID the coin table so we can iterate thru it's rows.
        coin_table = soup.find('table', attrs={'id': 'currencies-all'})
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
        return links_to_coins  # Sending the links back for further parsing!

    def scrape_data_for_coin(self, link_to_coin):
        soup = soupify(self.session, link_to_coin + 'historical-data/' + '?start=20000101&end=21000101')
        # print(link_to_coin + 'historical-data/' + '?start=20000101&end=21000101')
        #  We have to add 'historical-data' to the link, or else
        #  we'll just get the data available on the currency page.
        # Additionally, setting the search range from 2000 to 2100
        # automatically gives us all the available data.
        coin_name = soup.find('h2').text.strip().replace(
            'Historical data for ', ''
        )  # This gives us the name of the coin.
        historical_data_table = soup.find('table', class_='table')
        #  Just like in scrape_coin_links(), we find the table
        #  and then iterate over it - skipping the headers again.
        c = Coin(name=coin_name)
        for row in historical_data_table.find_all('tr')[1:]:
            cells = row.find_all('td')
            #  And just like in scrape_coin_links, we gather all
            #  of the row's data held in td tags.
            c.date_.append(cells[0].text.strip())
            c.open_.append(cells[1].text.strip())
            c.high.append(cells[2].text.strip())
            c.low.append(cells[3].text.strip())
            c.close.append(float(cells[4].text.strip()))
            c.volume.append(cells[5].text.strip())
            c.market_cap.append(cells[6].text.strip())
        soup = soupify(self.session, link_to_coin +
                       '#markets')
        table = soup.find('table', attrs={'id': 'markets-table'})
        for row in table.find_all('tr')[1:]:
            cells = [i.text for i in row.find_all('td')]
            c.exchanges.append(cells[1])

        # print c.close
        lim = LimitCoinMarketCap(c.volume, c.close)
        data = c.close[lim::-1]
        # PrintListInColumn(data)
        KFull = KellyList(data)
        # a=zip(KFull,data,xrange(len(data)))
        # PrintListPercentageInColumn(a)
        # print c.name, " kelly=%1.2f%% volume=%1.2f"%((KFull[-1]*100),(float(c.volume[-1])*c.close[-1]))
        print c.name, " kelly=%1.2f%%" % ((KFull[-1] * 100))
        print
        c.writer()


def file_handler():
    """
    Creates the file we'll write to, and writes headers.
    :return: None
    """
    with open('coinmarketcap_results.csv', 'w') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(
            ['name', 'date_', 'open_', 'high', 'low',
             'close', 'volume', 'market_cap', 'exchanges']
        )


def main():
    file_handler()
    coinmarketcap_scraper = CoinMarketCap_Scraper()
    links_to_coins = coinmarketcap_scraper.scrape_coin_links()
    for link_to_coin in tqdm(links_to_coins):
        try:
            coinmarketcap_scraper.scrape_data_for_coin(link_to_coin)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
