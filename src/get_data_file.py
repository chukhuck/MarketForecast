from yahoofinancials import YahooFinancials
from pprint import pprint
import json
from datetime import date
from pathlib import Path


def get_filename(ticket_name: str, ticket: str, start_date: str, end_date: str, step: str):
    filename = '..\data\{}.json'.format(ticket_name)
    if not Path(filename).is_file():
        return download_file(filename, ticket, start_date, end_date, step)

    return filename


def download_file(filename: str, ticket: str, start_date: str, end_date: str, step: str):
    print('Get historical price data for {}'.format(ticket))
    yahoo_financials = YahooFinancials(ticket)

    historical_stock_prices = yahoo_financials.get_historical_price_data(
        start_date, end_date, step)

    with open(filename, 'w+') as fp:
        json.dump(historical_stock_prices[ticket]['prices'], fp)
    return filename
