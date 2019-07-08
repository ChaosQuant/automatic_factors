# -*- coding: utf-8 -*-

import pdb
import numba
import six
import pandas as pd
import numpy as np
from numpy import log
from sqlalchemy import create_engine
from sqlalchemy import select, and_
from singleton import Singleton
from model import Market

def stddev(df, window=10):
    """
    Wrapper function to estimate rolling standard deviation.
    滑动窗口求标准差
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series min over the past 'window' days.
    """
    return df.rolling(window).std()

def ts_argmax(df, window=10):
    """
    Wrapper function to estimate which day ts_max(df, window) occurred on
    滑动窗口中的数据最大值位置
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: well.. that :)
    """
    return df.rolling(window).apply(np.argmax) + 1

def delta(df, window=10, period=1):
    """
    Wrapper function to estimate difference.
    按参数求一列时间序列数据差值，period=1，今日减去昨日，以此类推
    :param df: a pandas DataFrame.
    :param period: the difference grade.
    :return: a pandas DataFrame with today’s value minus the value 'period' days ago.
    """
    return df.rolling(window).diff(period)

def rank(df,window=10):
    """
    Cross sectional rank
    排序，返回排序百分比数
    :param df: a pandas DataFrame.
    :return: a pandas DataFrame with rank along columns.
    """
    return df.rank(axis=1, pct=True)

def correlation(x, y, window=10):
    """
    Wrapper function to estimate rolling corelations.
    滑动窗口求相关系数
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series min over the past 'window' days.
    """
    return x.rolling(window).corr(y)

#获取数据区间避免大量数据反复计算
#def get_basic_data(data, )

@six.add_metaclass(Singleton)
class Alpha101(object):
    def __init__(self):
        __str__ = 'alpha101'
    
    #(rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) -0.5)
    def alpha001(self, data, end_date, param1=20, param2=2, param3=5, param4=0.5):
        pdb.set_trace()
        inner = data[['close_price','returns']].copy()
        inner.close_price[data.returns < 0] = stddev(inner.returns, param1)
        result =  ts_argmax(inner.close_price ** param2, param3).rank(axis=1, pct=True) - param4
        result = result.where(pd.notnull(result), None)
        return result[-param3:]
    
    #(-1*correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
    def alpha002(self, data, param1=2, param2=6):
        pdb.set_trace()
        inner = data[['turnover_vol','close_price','open_price']].copy()
        result = -1 * correlation(rank(delta(log(inner.turnover_vol), param1)[param1:]), 
                              rank((inner.close_price - inner.open_price) / inner.open_price)[param1:], param2)[param2:]
        result = result.where(pd.notnull(result), None)
        return result
    
    ## (-1 * correlation(rank(open), rank(volume), 10))
    def alpha003(self, data, param1):
        inner = data[['turnover_vol','open_price']].copy()
        result =  -1 * correlation(rank(self.open), rank(self.volume), 10)
        result = result.where(pd.notnull(result), None)
        return result
        

if __name__ == "__main__":
    start_date = '2018-10-01'
    end_date = '2018-12-30'
    stock_list = [600519,858,799,2304,860,603369]
    sql_engine = create_engine("postgresql+psycopg2://alpha:alpha@180.166.26.82:8889/alpha")
    query = select([Market]).where(
            and_(
                Market.trade_date < end_date,
                Market.trade_date > start_date,
                Market.code.in_(stock_list)
            )
        )
    df = pd.read_sql(query, con=sql_engine)
    df.rename(columns={'preClosePrice':'pre_close','openPrice':'open_price',
                      'highestPrice':'highest_price','lowestPrice':'lowest_price',
                      'closePrice':'close_price','turnoverVol':'turnover_vol',
                      'turnoverValue':'turnover_value'}, inplace=True)
    dependencies = ['close_price', 'open_price', 'highest_price', 'lowest_price', 'pre_close', 
                    'turnover_vol', 'turnover_value', 'trade_date', 'code']
    ret = df[dependencies].set_index(['trade_date', 'code']).to_panel()
    ret['returns'] = ret['close_price'] / ret['pre_close'] - 1
    ret['vwap'] = (ret['turnover_value'] * 1000) / (ret['turnover_vol'] * 100 + 1)
    Alpha101().alpha001(ret,'2018-12-28')
    
    print('---')