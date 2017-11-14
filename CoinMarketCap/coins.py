from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('sqlite:///coinmarketcap.db')


def kelly_index(close_values):
    # data = self.history[['Date', 'Close']].sort_values(by=['Date'])
    # data = self.history.sort_values(by=['Date'],ascending=False)
    # data = close_values
    # print data
    # data = data['Close']

    data = [x for x in close_values]
    # data = data[::-1]
    # pprint (data)

    if len(data) < 2:
        return []
    kelly_list = [0.0]
    number_positives = number_negatives = pos_delta = neg_delta = average_raise = average_drop = 0.0

    for t in xrange(len(data) - 1):
        old_value = data[t]
        new_value = data[t + 1]
        diff = new_value - old_value
        DiffInFraction = diff / old_value
        DiffInPercentage = DiffInFraction * 100.0

        if diff > 0:
            number_positives += 1.0
            pos_delta += DiffInPercentage
            average_raise = pos_delta / number_positives
        elif diff < 0:
            number_negatives += 1.0
            neg_delta += DiffInPercentage
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
    # self.kelly_index_value = kl[0] * 100
    return kl[0] * 100


def close_dates():
    q = "select distinct Coin, Date, Close from history order by Date" #   desc"
    df = pd.read_sql(q, engine, parse_dates=['Date'])
    return df

df = close_dates()
grouped_df = df.groupby('Coin')
# grouped_df.get_group('bitcoin')

for name, group in grouped_df:
    print('{}: {}'.format(name, kelly_index(group['Close'])))

