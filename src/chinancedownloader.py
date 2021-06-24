from moex import MOEX
import datetime
from datetime import date
from urllib import request
from yahoofinancials import YahooFinancials
import pandas as pd
import os
import investpy
from pathlib import Path


def update_data(source: str, country: str, ticket: str, date_from: date, date_to: date, filename: str, currency: str = 'UNK'):

    if source is 'moex':
        update_from_moex(filename=filename,
                         ticket=ticket, date_from=date_from, date_to=date_to)
    elif source is 'stooq':
        update_from_stooq(filename=filename,
                          ticket=ticket, date_from=date_from, date_to=date_to, currency=currency)
    elif source is 'msci':
        update_from_msci(filename=filename,
                         ticket=ticket, date_from=date_from, date_to=date_to)
    elif source is 'yahoo':
        update_from_yahoo(filename=filename,
                          ticket=ticket, date_from=date_from, date_to=date_to, currency=currency)
    elif source is 'investing':
        update_from_investing(filename=filename, country=country,
                              ticket=ticket, date_from=date_from, date_to=date_to)
    else:
        raise ValueError('Unknown data source')

    return filename + '.csv'


def update_from_investing(filename: str, country: str,  ticket: str, date_from: date, date_to: date):
    df = investpy.get_index_historical_data(index=ticket,
                                            country=country,
                                            from_date=date_from.strftime(
                                                '%d/%m/%Y'),
                                            to_date=date_to.strftime('%d/%m/%Y'))

    df.reset_index(inplace=True)
    df.rename(columns={'Date': 'date', 'Open': 'open',
                       'High': 'high', 'Low': 'low',
                       'Close': 'close', 'Volume': 'volume',
                       'Currency': 'currency', }, inplace=True)

    if 'capitalization' not in df.columns:
        df = df.assign(capitalization=0.0)

    df = df.assign(secid='investing:' + ticket)

    df = df[['secid', 'currency', 'date', 'close', 'open',
             'low', 'high', 'volume', 'capitalization']]

    df.set_index(['date'], inplace=True)

    path_to_file = '..\\data\\{}.csv'.format(filename)
    df.to_csv(path_to_file,
              mode='a', header=(not Path(path_to_file).is_file()))


def update_from_moex(filename: str, ticket: str, date_from: date, date_to: date):
    moex = MOEX()
    data = moex.history_engine_market_security(
        date_start=date_from.strftime('%Y-%m-%d'), date_end=date_to.strftime('%Y-%m-%d'), security=ticket)

    df = data[["TRADEDATE", "SECID", "OPEN", "CLOSE",
               "LOW", "HIGH", "VALUE", "CURRENCYID", 'CAPITALIZATION']]

    df.rename(columns={'TRADEDATE': 'date', 'OPEN': 'open',
                       'HIGH': 'high', 'LOW': 'low', 'CLOSE': 'close',
                       'SECID': 'secid',  'VALUE': 'volume',
                       'CURRENCYID': 'currency',  'CAPITALIZATION': 'capitalization', }, inplace=True)

    df = df[['secid', 'currency', 'date', 'close', 'open',
             'low', 'high', 'volume', 'capitalization']]

    df.set_index(['date'], inplace=True)

    path_to_file = '..\\data\\{}.csv'.format(filename)
    df.to_csv(path_to_file,
              mode='a', header=(not Path(path_to_file).is_file()))


def update_from_stooq(filename: str, ticket: str, date_from: date, date_to: date, currency='UNK'):
    date_start = date_from.strftime('%Y%m%d')
    date_end = date_to.strftime('%Y%m%d')
    url = 'https://stooq.com/q/d/l/?s={0}&d1={1}&d2={2}&i=d'.format(
        ticket, date_start, date_end)

    df = pd.read_csv(url)
    df.rename(columns={'Date': 'date', 'Open': 'open',
                       'High': 'high', 'Low': 'low',
                       'Close': 'close', 'Volume': 'volume'}, inplace=True)

    if 'volume' not in df.columns:
        df = df.assign(volume=0.0)

    if 'capitalization' not in df.columns:
        df = df.assign(capitalization=0.0)

    df = df.assign(currency='USD', secid='STOOQ:' + ticket)

    df = df[['secid', 'currency', 'date', 'close', 'open',
             'low', 'high', 'volume', 'capitalization']]

    df.set_index(['date'], inplace=True)

    path_to_file = '..\\data\\{}.csv'.format(filename)
    df.to_csv(path_to_file,
              mode='a', header=(not Path(path_to_file).is_file()))


def update_from_msci(filename: str, ticket: str, date_from: date, date_to: date):
    date_start = date_from.strftime('%Y%m%d')
    date_end = date_to.strftime('%Y%m%d')
    url = 'https://app2.msci.com/products/service/index/indexmaster/downloadLevelData?output=INDEX_LEVELS&currency_symbol=USD&index_variant=STRD&start_date={0}&end_date={1}&data_frequency=DAILY&baseValue=false&index_codes={2}'.format(
        date_start, date_end, ticket)
    print(url)
    content = request.urlopen(url).read()
    temp = content
    with open('..\\data\\msci_temp.xls', 'wb+') as fp:
        fp.write(content)

    xls = pd.read_excel('..\\data\\msci_temp.xls', engine='xlrd')
    xls = xls.drop(xls.index[[0, 1, 2, 3, 4, 5]])
    xls = xls.drop(
        xls.index[list(range(len(xls.index) - 19, len(xls.index)))])
    df = pd.DataFrame({'date': [datetime.datetime.strptime(d, "%b %d, %Y").date() for d in xls['Unnamed: 0'].values],
                       'secid':         'MSCI:' + filename,
                       'close':          xls['Unnamed: 1'].values,
                       'open':           0.0,
                       'low':            0.0,
                       'high':           0.0,
                       'volume':         0.0,
                       'capitalisation': 0.0,
                       'currency':      'USD'})

    df = df[['secid', 'currency', 'date', 'close', 'open',
             'low', 'high', 'volume', 'capitalization']]

    df.set_index(['date'], inplace=True)

    path_to_file = '..\\data\\{}.csv'.format(filename)
    df.to_csv(path_to_file,
              mode='a', header=(not Path(path_to_file).is_file()))

    if(os.path.isfile('..\\data\\msci_temp.xls')):
        os.remove('..\\data\\msci_temp.xls')


def update_from_yahoo(filename: str, ticket: str, date_from: date, date_to: date, currency='UNK'):
    yahoo_financials = YahooFinancials(ticket)
    historical_stock_prices = yahoo_financials.get_historical_price_data(
        date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d'), 'daily')

    df = pd.DataFrame(historical_stock_prices[ticket]['prices'])
    df = df.drop(['date', 'adjclose'], axis=1)
    df = df.rename(columns={'formatted_date': 'date'})

    df = df.assign(capitalization=0.0,
                   currency=historical_stock_prices[ticket]['currency'], secid='yahoo:' + ticket)

    df = df[['secid', 'currency', 'date', 'close', 'open',
             'low', 'high', 'volume', 'capitalization']]

    df.set_index(['date'], inplace=True)

    path_to_file = '..\\data\\{}.csv'.format(filename)
    df.to_csv(path_to_file,
              mode='a', header=(not Path(path_to_file).is_file()))
