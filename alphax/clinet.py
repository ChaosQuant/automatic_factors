import pdb
import json
import copy
import inspect
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import select, and_
from PyFin.api import advanceDateByCalendar, bizDatesList
from alphax.alpha191 import Alpha191
from alphax.alpha101 import Alpha101
from alphax.model import Market,Exposure

def test_calc101():
    INDU_STYLES = ['Bank','RealEstate','Health','Transportation','Mining','NonFerMetal',
                   'HouseApp','LeiService','MachiEquip','BuildDeco','CommeTrade','CONMAT',
                   'Auto','Textile','FoodBever','Electronics','Computer','LightIndus',
                   'Utilities','Telecom','AgriForest','CHEM','Media','IronSteel',
                   'NonBankFinan','ELECEQP','AERODEF']
    alpha_number = 101
    engine = create_engine('')
    begin_date = '2018-12-01'
    end_date = '2018-12-28'
    query = select([Market]).where(
            and_(Market.trade_date >= begin_date, Market.trade_date <= end_date, ))
    mkt_df = pd.read_sql(query, engine)
    mkt_df.rename(columns={'preClosePrice':'pre_close','openPrice':'open_price',
                      'highestPrice':'highest_price','lowestPrice':'lowest_price',
                      'closePrice':'close_price','turnoverVol':'turnover_vol',
                      'turnoverValue':'turnover_value','accumAdjFactor':'accum_adj',
                      'vwap':'vwap','negMarketValue':'neg_mkt_value','marketValue':'mkt_value',
                      'chgPct':'chg_pct','PE':'pe_ttm','PE1':'pe','PB':'pb'}, inplace=True)
    mkt_df = mkt_df[[('000000' + str(code))[-6:][0] in '036' for code in mkt_df['code']]]
    trade_date_list = list(set(mkt_df.trade_date))
    trade_date_list.sort(reverse=True)
    mkt_df = mkt_df[mkt_df['turnover_vol'] > 0]
    for p in mkt_df.columns:
        if p in ['open_price', 'highest_price', 'lowest_price', 'close_price', 'vwap']:
            mkt_df[p+'_raw'] = mkt_df[p]
            mkt_df[p] = mkt_df[p] * mkt_df['accum_adj']
    # multiplier for pe, pb, mkt_value and neg_mkt_value
    mkt_multiplier = mkt_df[['trade_date', 'code', 'accum_adj', 'close_price', 
                             'neg_mkt_value', 'mkt_value', 'pe_ttm', 'pe', 'pb']]
    for p in ['neg_mkt_value', 'mkt_value', 'pe_ttm', 'pe', 'pb']:
        mkt_multiplier[p] = mkt_multiplier[p] / mkt_multiplier['close_price']
    mkt_multiplier.drop('close_price', axis=1, inplace=True)
    mkt_df.drop(['accum_adj', 
                 'neg_mkt_value', 'mkt_value', 'pe_ttm', 'pe', 'pb'], axis=1, inplace=True)
    mkt_df = mkt_df.merge(mkt_multiplier, on=['trade_date', 'code'], how='left')
    mkt_df['turnover'] = mkt_df['turnover_value'] / mkt_df['mkt_value']
    mkt_df = mkt_df.set_index(['trade_date', 'code'])
    
    
    query = select([Exposure]).where(
            and_(Exposure.trade_date >= begin_date, Exposure.trade_date <= end_date))
    risk_df = pd.read_sql(query, engine)
    risk_df = risk_df[[('000000' + str(code))[-6:][0] in '036' for code in risk_df['code']]]
    risk_df = risk_df.sort_values(['trade_date', 'code']).reset_index(drop=True)

    date_list = bizDatesList('China.SSE', begin_date, end_date)
    indu_dict = {}
    indu_names = INDU_STYLES + ['COUNTRY']
    for date in date_list:
        date_indu_df = risk_df[risk_df['trade_date'] == date_list[-1]].set_index('code')[indu_names]
        indu_check_se = date_indu_df.sum(axis=1).sort_values()
        date_indu_df.drop(indu_check_se[indu_check_se < 2].index, inplace=True)
        indu_dict[date] = date_indu_df.sort_index()
    # total dataframe to dataframe dict
    total_data = {}
    for col in ['open_price', 'highest_price', 'lowest_price', 'close_price', 
                'vwap', 'turnover_vol','turnover_value',
                'neg_mkt_value','mkt_value',  'pe_ttm', 'pe', 'pb',
                'open_price_raw', 'highest_price_raw', 'lowest_price_raw', 
                'close_price_raw', 'vwap_raw',
                'accum_adj']:
        total_data[col] = mkt_df[col].unstack().sort_index()
    total_data['returns'] = total_data['close_price'].pct_change()
    total_data['indu'] = indu_dict
    #58, 59, 67,69, 76, 80, 82, 87, 90, 91， 97
    alpha_num_list = [58, 59, 67, 69, 76, 80, 82, 87, 90, 91, 97]
    pdb.set_trace()
    for date in trade_date_list:
        for number in alpha_num_list:
            print('Alpha101().alpha_' + str(number), date)
            alpha_fun = eval('Alpha101().alpha_' + str(number))
            fun_param = inspect.signature(alpha_fun).parameters
            dependencies = fun_param['dependencies'].default
            max_window = fun_param['max_window'].default
        #若在外部指定参数则可计算出max_windows
            begin = advanceDateByCalendar('china.sse', date, '-%sb' % (max_window - 1))
            data = {}
            for dep in dependencies:
                data[dep] = total_data[dep].loc[begin:date]
                data['indu'] = total_data['indu']
            alpha_fun(data)
    
def test_calc191():
    alpha_number = 191
    engine = create_engine('')
    begin_date = '2018-12-01'
    end_date = '2018-12-28'
    query = select([Market]).where(
            and_(Market.trade_date >= begin_date, Market.trade_date <= end_date, ))
    mkt_df = pd.read_sql(query, engine)
    mkt_df.rename(columns={'preClosePrice':'pre_close','openPrice':'open_price',
                      'highestPrice':'highest_price','lowestPrice':'lowest_price',
                      'closePrice':'close_price','turnoverVol':'turnover_vol',
                      'turnoverValue':'turnover_value','accumAdjFactor':'accum_adj',
                       'vwap':'vwap'}, inplace=True)
    mkt_df = mkt_df[[('000000' + str(code))[-6:][0] in '036' for code in mkt_df['code']]]
    trade_date_list = list(set(mkt_df.trade_date))
    trade_date_list.sort(reverse=True)
    mkt_df = mkt_df.set_index(['trade_date', 'code'])
    mkt_df = mkt_df[mkt_df['turnover_vol'] > 0]
    # backward adjustment of stock price
    for p in mkt_df.columns:
        if p in ['open_price', 'highest_price', 'lowestPrice', 'close_price', 'vwap']:
            mkt_df[p] = mkt_df[p] * mkt_df['accum_adj']
    total_data = mkt_df.to_panel()
    for date in trade_date_list:
        for number in range(1, alpha_number+1):
            if number==30:
                continue
            print('Alpha191().alpha_' + str(number), date)
            alpha_fun = eval('Alpha191().alpha_' + str(number))
            fun_param = inspect.signature(alpha_fun).parameters
            dependencies = fun_param['dependencies'].default
            max_window = fun_param['max_window'].default
            #若在外部指定参数则可计算出max_windows
            begin = advanceDateByCalendar('china.sse', date, '-%sb' % (max_window - 1))
            data = {}
            pdb.set_trace()
            for dep in dependencies:
                data[dep] = total_data[dep].loc[begin:date]
            alpha_fun(data)
        print("elapse time %s ..." % (datetime.datetime.now() - start)) 
        break # 只测试一天
        
        
if __name__ == "__main__":
    test_calc101()
    print('----')
