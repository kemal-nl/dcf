from yahoo_fin.stock_info import get_income_statement, get_cash_flow, get_analysts_info, get_stats, tickers_sp500
from yfinance import Ticker
import requests 
import pandas as pd
import datetime
import time
import json
import numpy as np
import numpy_financial as npf
import currency_converter as cc
import concurrent.futures
from tqdm import tqdm
import csv


def get_fundamental_data(ticker, payload):
    period1 = 493590046
    period2 = get_unix_timestamp()
    url_payload = build_url_string_payload(payload)
    base_url = 'http://query1.finance.yahoo.com/'
    fundamentals_url = f'ws/fundamentals-timeseries/v1/finance/timeseries/{ticker}?lang=en-US&region=US&symbol={ticker}&padTimeSeries=true&type={url_payload}&merge=false&period1={period1}&period2={period2}&corsDomain=finance.yahoo.com'
    
    try:
        time.sleep(5)
        data = json.loads(requests.get(base_url + fundamentals_url).text)
    except:
        print(f'Could not download data for {ticker_input}')
        return
    
    return data


def get_unix_timestamp(date_=datetime.date.today()):
    return int(time.mktime(date_.timetuple()))


def build_url_string_payload(payload):
    url_payload = payload[0]
    if len(payload)>1:
        for s in payload[1:]:
            url_payload+=f',{s}'
    return url_payload


def filter_ticker(ticker):
    '''Download all tickers that have consistently growing 
    cashflow over past 5 years'''

    time.sleep(5)

    try:
        income_statement = get_income_statement(ticker)
        revenue = income_statement.loc['totalRevenue']
        net_income = income_statement.loc['netIncome']
        cash_flow_from_ops = get_cash_flow(ticker).loc['totalCashFromOperatingActivities']
    except:
        return False
    return (consistent_growth(revenue) and consistent_growth(net_income) and consistent_growth(cash_flow_from_ops))


def consistent_growth(list_input):
    
    # first check that all values are positive
    if len(list_input[list_input < 0]) > 0:
        return False
    
    # check consistent growth
    if (list_input[0] > list_input[1]):
        cond_1 = 1
    else: 
        cond_1 = 0
    if (list_input[1] > list_input[2]):
        cond_2 = 1
    else: 
        cond_2 = 0
    if (list_input[2] > list_input[3]):
        cond_3 = 1
    else: 
        cond_3 = 0

    if (cond_1 + cond_2 + cond_3) >= 2:
        return True
    else:
        return False

def calculate_dcf(ticker_input):
    # No. of Shares Outstanding
    t = Ticker(ticker_input)
    
    try:
        shares = t.info['sharesOutstanding']
    except:
        return
    last_close_price = t.info['previousClose']
    # print(f"Number of shares outstanding: {shares}")

    # Total Debt and Cash, Cash Equivalents amd Short Term Investments
    payload = ['quarterlyTotalDebt', 'quarterlyCashCashEquivalentsAndShortTermInvestments']
    fundamental_json_dict = get_fundamental_data(ticker_input, payload)

    try:
        fundamental_data = {
            'currency':
            fundamental_json_dict['timeseries']['result'][0][fundamental_json_dict
                                                             ['timeseries']['result'][0]['meta']['type'][0]][-1]['currencyCode'],
            fundamental_json_dict['timeseries']['result'][0]['meta']['type'][0]: fundamental_json_dict['timeseries']['result'][0][fundamental_json_dict['timeseries']['result'][0]['meta']['type'][0]][-1]['reportedValue']['raw'],
            fundamental_json_dict['timeseries']['result'][1]['meta']['type'][0]: fundamental_json_dict['timeseries'][
                'result'][1][fundamental_json_dict['timeseries']['result'][1]['meta']['type'][0]][-1]['reportedValue']['raw']
        }
    except:
        return

    # Cash Flow from operations over last 4 quarters
    cashflow = get_cash_flow(ticker_input, yearly=False)
    cashflow_operating_current = cashflow.loc['totalCashFromOperatingActivities'].sum()

    # print(f"Operating Cash Flow (Last 4 quarters): {cashflow_operating_current} {fundamental_data['currency']}")
    # print(f"{payload[0]} (last quarter): {fundamental_data[payload[0]]} {fundamental_data['currency']}")
    # print(f"{payload[1]} (last quarter): {fundamental_data[payload[1]]} {fundamental_data['currency']}")

    # get 5 yr cash flow growth estimate
    growth_estimate_1_5 =  float(get_analysts_info(ticker_input)['Growth Estimates'].iloc[4,1].split('%')[0])/100
    growth_estimate_6_10 = growth_estimate_1_5/2
    # print(f'Growth estimate: {growth_estimate_1_5}')

    cashflows_1_5 = np.array([cashflow_operating_current*(1+growth_estimate_1_5)**year for year in range(1, 6)])
    cashflows_6_10 = np.array([cashflows_1_5[-1]*(1+growth_estimate_6_10)**year for year in range(1, 6)])
    cashflows = np.concatenate((cashflows_1_5, cashflows_6_10))
    # print(cashflows)

    # get beta
    beta = t.info['beta']

    if beta < 0.8:
        discount_rate = 0.05
    elif beta < 1:
        discount_rate = 0.06
    elif beta < 1.1:
        discount_rate = 0.065
    elif beta < 1.2:
        discount_rate = 0.07
    elif beta < 1.3:
        discount_rate = 0.075
    elif beta < 1.4:
        discount_rate = 0.08
    elif beta < 1.5:
        discount_rate = 0.085
    elif beta >= 1.5:
        discount_rate = 0.09

    # calculate Net Present Value of future cashflows
    present_value_cashflows = npf.npv(discount_rate, cashflows)
    intrinsic_value_cashflows = present_value_cashflows/shares

    # calcualte debt and cash per share
    debt_per_share = fundamental_data['quarterlyTotalDebt']/shares
    cash_per_share = fundamental_data['quarterlyCashCashEquivalentsAndShortTermInvestments']/shares

    # Intrinsic Value of the stock in local currency
    intrinsic_value = intrinsic_value_cashflows - debt_per_share + cash_per_share

    # if currency not USD, convert to USD
    if fundamental_data['currency'] != 'USD':
        c = cc.currency_converter.CurrencyConverter()
        intrinsic_value = c.convert(intrinsic_value, fundamental_data['currency'], 'USD')
        
    # print(f'The intrinsic value for {ticker_input} is: {intrinsic_value:.2f} USD')
    # print(f'The last close price for {ticker_input} is: {last_close_price:.2f} USD')
    # print(f'The Margin of Safety for {ticker_input} is: {(intrinsic_value-last_close_price)/intrinsic_value*100:.2f}%')

    MS = (intrinsic_value-last_close_price)/intrinsic_value*100

    return [ticker_input, intrinsic_value, last_close_price, MS]


if __name__=="__main__":

    # download all tickers from S&P 500
    tickers = tickers_sp500()

    # filter tickers
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     filtered_list = list(tqdm(executor.map(filter_ticker, tickers), total = len(tickers)))
    # filtered_tickers = [tickers[i] for i in range(len(tickers)) if filtered_list[i] == True]
    # pd.DataFrame(filtered_tickers).to_csv('filtered_tickers.csv')

    filtered_tickers = list(pd.read_csv('filtered_tickers.csv', index_col=0)['0'])

    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     dcf_list = list(tqdm(executor.map(calculate_dcf, filtered_tickers), total = len(filtered_tickers)))
    

    data = [calculate_dcf(ticker) for ticker in tqdm(filtered_tickers)]
    data = [ticker for ticker in data if ticker is not None]
    dfc_data = pd.DataFrame(data)
    dfc_data.set_index(0, inplace=True)
    dfc_data.columns = ['Intrinsic Value', 'Last Close Price', 'Margin of Safety']
    dfc_data.index.names = ['Ticker']

    dfc_data.to_excel('DFC results.xlsx')
