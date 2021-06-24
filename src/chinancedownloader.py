from moex import MOEX
import datetime
from datetime import date
from urllib import request
from pandas.core.frame import DataFrame
from yahoofinancials import YahooFinancials
import pandas as pd
import os
import investpy
from pathlib import Path


def get_or_update_data(source: str, country: str, ticket: str, date_from: date, date_to: date, filename: str, currency: str = 'UNK'):

    try:
        if source is 'moex':
            data = get_data_from_moex(
                ticket=ticket, date_from=date_from, date_to=date_to)
        elif source is 'stooq':
            data = get_data_from_stooq(
                ticket=ticket, date_from=date_from, date_to=date_to, currency=currency)
        elif source is 'msci':
            data = get_data_from_msci(
                ticket=ticket, date_from=date_from, date_to=date_to)
        elif source is 'yahoo':
            data = get_data_from_yahoo(
                ticket=ticket, date_from=date_from, date_to=date_to, currency=currency)
        elif source is 'investing':
            data = get_data_from_investing(ticket=ticket, country=country,
                                           date_from=date_from, date_to=date_to)
        else:
            raise ValueError('Unknown data source')
    except RuntimeError:
        data = DataFrame(columns={'date', 'secid', 'close', 'open',
                                  'low', 'high', 'volume', 'capitalisation', 'currency'})
    except ValueError:
        raise ValueError('Unknown data source or another')
    except:
        data = pd.DataFrame(columns={'date', 'secid', 'close', 'open',
                                     'low', 'high', 'volume', 'capitalisation', 'currency'})

    data = set_good_look(data)
    save_to_csv(data, filename=filename)

    return filename + '.csv'


def get_data_from_investing(ticket: str, country: str, date_from: date, date_to: date):
    data = investpy.get_index_historical_data(index=ticket,
                                              country=country,
                                              from_date=date_from.strftime(
                                                  '%d/%m/%Y'),
                                              to_date=date_to.strftime('%d/%m/%Y'))
    data.reset_index(inplace=True)
    data.rename(columns={'Date': 'date', 'Open': 'open',
                         'High': 'high', 'Low': 'low',
                         'Close': 'close', 'Volume': 'volume',
                         'Currency': 'currency', }, inplace=True)
    data = data.assign(secid='investing:' + ticket)

    return data


def get_data_from_moex(ticket: str, date_from: date, date_to: date):
    moex = MOEX()
    data = moex.history_engine_market_security(
        date_start=date_from.strftime('%Y-%m-%d'), date_end=date_to.strftime('%Y-%m-%d'), security=ticket)

    data = data[["TRADEDATE", "SECID", "OPEN", "CLOSE",
                 "LOW", "HIGH", "VALUE", "CURRENCYID", 'CAPITALIZATION']]

    data.rename(columns={'TRADEDATE': 'date', 'OPEN': 'open',
                         'HIGH': 'high', 'LOW': 'low', 'CLOSE': 'close',
                         'SECID': 'secid',  'VALUE': 'volume',
                         'CURRENCYID': 'currency',  'CAPITALIZATION': 'capitalization', }, inplace=True)
    return data


def get_data_from_stooq(ticket: str, date_from: date, date_to: date, currency='UNK'):
    url = 'https://stooq.com/q/d/l/?s={0}&d1={1}&d2={2}&i=d'.format(
        ticket, date_from.strftime('%Y%m%d'), date_to.strftime('%Y%m%d'))

    data = pd.read_csv(url)
    data.rename(columns={'Date': 'date', 'Open': 'open',
                         'High': 'high', 'Low': 'low',
                         'Close': 'close', 'Volume': 'volume'}, inplace=True)

    data = data.assign(
        currency='USD',
        secid='STOOQ:' + ticket)

    return data


def get_data_from_msci(ticket: str, date_from: date, date_to: date):
    url = 'https://app2.msci.com/products/service/index/indexmaster/downloadLevelData?output=INDEX_LEVELS&currency_symbol=USD&index_variant=STRD&start_date={0}&end_date={1}&data_frequency=DAILY&baseValue=false&index_codes={2}'.format(
        date_from.strftime('%Y%m%d'), date_to.strftime('%Y%m%d'), ticket)
    content = request.urlopen(url).read()
    temp = content
    with open('..\\data\\msci_temp.xls', 'wb+') as fp:
        fp.write(content)

    xls = pd.read_excel('..\\data\\msci_temp.xls', engine='xlrd')
    xls = xls.drop(xls.index[[0, 1, 2, 3, 4, 5]])
    xls = xls.drop(
        xls.index[list(range(len(xls.index) - 19, len(xls.index)))])
    data = pd.DataFrame({'date': [datetime.datetime.strptime(d, "%b %d, %Y").date() for d in xls['Unnamed: 0'].values],
                         'secid':         'MSCI:' + ticket,
                         'close':          xls['Unnamed: 1'].values,
                         'open':           0.0,
                         'low':            0.0,
                         'high':           0.0,
                         'volume':         0.0,
                         'capitalisation': 0.0,
                         'currency':      'USD'})

    if(os.path.isfile('..\\data\\msci_temp.xls')):
        os.remove('..\\data\\msci_temp.xls')

    return data


def get_data_from_yahoo(ticket: str, date_from: date, date_to: date, currency='UNK'):
    yahoo_financials = YahooFinancials(ticket)
    historical_stock_prices = yahoo_financials.get_historical_price_data(
        date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d'), 'daily')

    data = pd.DataFrame(historical_stock_prices[ticket]['prices'])
    data = data.drop(['date', 'adjclose'], axis=1)
    data = data.rename(columns={'formatted_date': 'date'})

    data = data.assign(
        currency=historical_stock_prices[ticket]['currency'],
        secid='yahoo:' + ticket)

    return data


def set_good_look(dataframe: DataFrame):
    if 'volume' not in dataframe.columns:
        dataframe = dataframe.assign(volume=0.0, inplace=True)

    if 'capitalization' not in dataframe.columns:
        dataframe = dataframe.assign(capitalization=0.0, inplace=True)

    dataframe = dataframe[['secid', 'currency', 'date', 'close', 'open',
                           'low', 'high', 'volume', 'capitalization']]

    dataframe.set_index(['date'], inplace=True)

    return dataframe


def save_to_csv(dataframe: DataFrame, filename: str):
    path_to_file = '..\\data\\{}.csv'.format(filename)
    dataframe.to_csv(path_to_file,
                     mode='a', header=(not Path(path_to_file).is_file()))
