import pdb
import json
import copy
import inspect
import pandas as pd
import numpy as np
import uuid
from sqlalchemy import create_engine
from sqlalchemy import select, and_
from sqlalchemy import create_engine, select, and_, or_
from sqlalchemy.pool import NullPool
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from PyFin.api import advanceDateByCalendar, bizDatesList
from alphax.alpha191 import Alpha191
from alphax.model import Market
import short_uuid 
    
class CustomClient(object):
    def __init__(self):
        __str__ = 'CustomClient'
        ##写入数据库
        destination = sa.create_engine("")
        self.destsession = sessionmaker( bind=destination, autocommit=False, autoflush=True)
 
    def update_destdb(self, table_name, sets):
        sets = sets.where(pd.notnull(sets), None)
        sql_pe = 'INSERT INTO {0} SET'.format(table_name)
        updates = ",".join( "{0} = :{0}".format(x) for x in list(sets) )
        sql_pe = sql_pe + '\n' + updates
        sql_pe = sql_pe + '\n' +  'ON DUPLICATE KEY UPDATE'
        sql_pe = sql_pe + '\n' + updates
        session = self.destsession()
        print('update_destdb:' + str(table_name))
        for index, row in sets.iterrows():
            dict_input = dict( row )
            dict_input['trade_date'] = dict_input['trade_date'].to_pydatetime()
            session.execute(sql_pe, dict_input)
        session.commit()
        session.close()
    
    def get_datasets(self, begin_date, end_date):
        alpha_number = 191
        engine = create_engine('')
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
        return total_data, trade_date_list
    
    def create_func(self, setting_file):
        func_list = []
        with open(setting_file,'rb') as f:
            content = f.read()
            json_ob = json.loads(content)
            for key,values in json_ob.items():
                alpha_fun_name = 'Alpha191().alpha_' + str(key)
                alpha_fun = eval(alpha_fun_name)
                fun_param = inspect.signature(alpha_fun).parameters
                dependencies = fun_param['dependencies'].default
                max_window = fun_param['max_window'].default
                alpha_fun_name += '(data=data'
                for params in values:
                    alpha_fun1 = copy.deepcopy(alpha_fun_name)
                    max_windows=0 
                    for pkey,pvalue in params.items():
                        alpha_fun1 += ',{0}={1}'.format(pkey, pvalue)
                        if 'param' in pkey:
                            max_windows += abs(pvalue)
                    alpha_fun1 += ')'
                    windows = max_windows if max_windows > max_window else max_window
                    func_list.append({'func':alpha_fun1,'dependencies':dependencies,
                                 'max_window':windows})
        return func_list

    ##自定义参数函数
    def custom_func(self, setting_file, begin_date, end_date):
        func_list = self.create_func(setting_file)
        total_data,trade_date_list = self.get_datasets(begin_date, end_date)
        for func_info in func_list:
            session_id = str(short_uuid.decode(short_uuid.uuid(func_info['func'])))
            dependencies = func_info['dependencies']
            max_window = func_info['max_window']
            func = func_info['func']
            for date in trade_date_list:
                begin = advanceDateByCalendar('china.sse', date, '-%sb' % (max_window - 1))
                data = {}
                for dep in dependencies:
                    data[dep] = total_data[dep].loc[begin:date]
                print(func,date)
                result = pd.DataFrame(eval(func),columns=['value'])
                result['session'] = session_id
                result['params'] = func_info['func']
                result['trade_date'] = date
                result = result.reset_index()
                self.update_destdb('train_factors', result)
                
if __name__ == "__main__":
    client = CustomClient()
    client.custom_func('Alpha191_param.json', '2017-01-01', '2018-12-28')
    
