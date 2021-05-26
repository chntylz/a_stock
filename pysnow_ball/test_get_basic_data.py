#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import pysnowball as ball
import datetime
import time

import sys 
sys.path.append("..") 


from Stocks import *
stocks=Stocks("usr","usr")


import numpy as np
import pandas as pd

from comm_interface import *

debug = 0
#debug = 1

token=get_cookie()
ball.set_token(token)

def get_stock_list():
    codestock_local=stocks.get_codestock_local()
    return codestock_local

def get_raw_data(stock_code, datatype=None, is_annuals=0, def_cnt=10):
    
    fina_data = None
    
    if datatype == 'income':
        fina_data = ball.income(stock_code, is_annuals, def_cnt)
    elif datatype == 'balance':
        fina_data = ball.balance(stock_code, is_annuals, def_cnt)
    elif datatype == 'cashflow':
        fina_data = ball.cash_flow(stock_code, is_annuals, def_cnt)
    else :
        fina_data = ball.indicator(stock_code, is_annuals, def_cnt)

    return fina_data



def get_fina_data(stock_code, datatype=None, is_annuals=0, def_cnt=10):

    fina_data = get_raw_data(stock_code, datatype, is_annuals, def_cnt)
    fina_data = fina_data['data']['list']

    df = pd.DataFrame() 
    new_df = pd.DataFrame() 

    if 0: 
        #first think, drop later
        s=str(fina_data)
        s=s.replace('[', '\'')
        s=s.replace(']', '\'')
        s=s.replace('None', '0')
        s1=s[1:len(s)-1]
        d=eval(s1)
        if debug:
            print(d)
        df = pd.DataFrame(d) 
    else:
        df = pd.DataFrame(fina_data) 

    if len(df):
        pass
    else:
        print('stock_code=%s, len(df)=0, #error# abnormal' \
                % stock_code)
        return df

    if debug:
        print(df.loc[len(df)-1])        #series
        print(df[len(df)-2:len(df)-1])  #dataframe


    len_cols = len(list(df)) 
    new_df = pd.DataFrame() 
    i = 0
    for i in range(3, len_cols):
        #split
        try:
            tmp_df = pd.DataFrame(data=[x[i] for x in df.values])
        except:
            print('try stock_code=%s, len(df)=%d, i=%d, #error# abnormal' \
                    % (stock_code, len(df), i))
            new_df = pd.DataFrame() 
            return new_df
        else:
            pass

        col_name = list(df)[i]
        tmp_df.rename(columns={0: col_name},inplace=True)
        tmp_df.rename(columns={1: col_name+'_new'},inplace=True)
        tmp_df.fillna(0, inplace=True)
        tmp_df = round(tmp_df, 4)
        if debug:
            print('col_name=%s'% col_name)
            print('tmp_df=%s\r'% tmp_df)

        if i == 3:
            new_df = tmp_df
        else:
            #new_df = pd.concat([new_df, tmp_df], axis=1, join_axes=[new_df.index])
            new_df = pd.concat([new_df, tmp_df], axis=1)


    if debug:
        print(df.head(1))
        print(list(df))
        print(new_df.head(1))
    
    #保留前3列，连接拆分出来的新df
    new_cols = ['report_date', 'report_name', 'ctime']
    df = df[new_cols]
    df = pd.concat([df, new_df], axis=1)

    return df


def get_holder_data(stock_code, def_cnt=10):

    his_data = ball.holders(stock_code, def_cnt)

    his_data = his_data['data']['items']
    df = pd.DataFrame(his_data)
    df = round(df, 2)
    
    if debug:
        print(df.loc[len(df)-1])        #series
        print(df[len(df)-2:len(df)-1])  #dataframe
    return df



def get_his_data(stock_code, def_cnt=1):

    data_list = []
    df = pd.DataFrame()
    his_data = ball.his_data(stock_code, def_cnt)

    #dict -> dict
    if 'data' in his_data:
        his_data = his_data['data']
    else:
        return df

    if 'symbol' in his_data:
        symbol = his_data['symbol']             
    else:
        return df

    #dict -> list
    column = his_data['column']
    col_len = len(column)

    df_column = column
    #df_column.append('symbol')

    if debug:
        print(df_column)

    #dict -> list
    his_data = his_data['item']

    df = pd.DataFrame(his_data, columns= df_column)
    
    if debug:
        print(df.loc[len(df)-1])        #series
        print(df[len(df)-2:len(df)-1])  #dataframe

    return df


def get_real_data(stock_code):
    rt_data = ball.quotec(stock_code)
    rawdata = rt_data['data']
    if rawdata[0] is None:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(rawdata)
        df=df.fillna(0)
        if 'timestamp' in df.columns:
            df['timestamp'] = df['timestamp'].apply(lambda x: get_date_from_timestamp(x))
    return df


def get_realtime_data(data_list, stock_code):
    rt_data = ball.quotec(stock_code)

    #dict -> list
    rt_data = rt_data['data']
    
    #list -> dict
    rt_data = rt_data[0]

    '''
    {
    'symbol': 'SZ000538', 
    'current': 111.98, 
    'percent': -2.23, 
    'chg': -2.55, 
    'timestamp': 1609139043000, 
    'volume': 10081979, 
    'amount': 1131364048.0, 
    'market_capital': 143043623438.0, 
    'float_market_capital': 67430655845.0, 
    'turnover_rate': 1.67, 
    'amplitude': 3.32, 
    'open': 114.9, 
    'last_close': 114.53, 
    'high': 114.9, 
    'low': 111.1, 
    'avg_price': 112.22, 
    'trade_volume': None, 
    'side': None, 
    'is_trade': False, 
    'level': 2, 
    'trade_session': None, 
    'trade_type': None, 
    'current_year_percent': 29.49, 
    'trade_unique_id': None, 
    'type': 11, 
    'bid_appl_seq_num': None, 
    'offer_appl_seq_num': None, 
    'volume_ext': None, 
    'traded_amount_ext': None}
    '''


    symbol=                 rt_data['symbol']             
    current=                rt_data['current']            
    percent=                rt_data['percent']            
    chg=                    rt_data['chg']                
    timestamp=              rt_data['timestamp']          
    volume=                 rt_data['volume']             
    amount=                 rt_data['amount']             
    market_capital=         rt_data['market_capital']     
    float_market_capital=   rt_data['float_market_capital']
    turnover_rate=          rt_data['turnover_rate']      
    amplitude=              rt_data['amplitude']          
    my_open=                rt_data['open']               
    last_close=             rt_data['last_close']         
    high=                   rt_data['high']               
    low=                    rt_data['low']                
    avg_price=              rt_data['avg_price']          
    trade_volume=           rt_data['trade_volume']       
    side=                   rt_data['side']               
    is_trade=               rt_data['is_trade']           
    level=                  rt_data['level']              
    trade_session=          rt_data['trade_session']      
    trade_type=             rt_data['trade_type']         
    current_year_percent=   rt_data['current_year_percent']
    trade_unique_id=        rt_data['trade_unique_id']    
    my_type=                rt_data['type']               
    bid_appl_seq_num=       rt_data['bid_appl_seq_num']   
    offer_appl_seq_num=     rt_data['offer_appl_seq_num'] 
    volume_ext=             rt_data['volume_ext']         
    traded_amount_ext=      rt_data['traded_amount_ext']  


    record_date = datetime.datetime.fromtimestamp(timestamp/1000).isoformat()
    record_date = record_date[:10]
    data_list.append([symbol, record_date, current, my_open, high, low, last_close, chg, \
            current_year_percent, volume])

    return data_list

def convert_daily_data_list_to_df():
    data_list = []
    codestock_local=get_stock_list()
    length=len(codestock_local)
    for i in range(0,length):
        nowcode=codestock_local[i][0]
        if nowcode[0:1] == '6':
            stock_code_new= 'SH' + nowcode
        else:                              
            stock_code_new= 'SZ' + nowcode 

        #rt_data = get_realtime_data(data_list, 'SZ000538')
        rt_data = get_realtime_data(data_list, stock_code_new)
        
        #print(rt_data)
    df_cols=['stock_code', 'record_date', 'close', 'open', 'high', 'low', 'last_close', 'chg', \
            'yoy_pct', 'volume']

    df = pd.DataFrame(data_list, columns=df_cols)

    return df
    

if __name__ == '__main__':
    

    print(time.localtime(time.time()))
    t1 = time.time()
    t2 = time.time()
    print("t1:%s, t2:%s, delta time=%s"%(t1, t2, t2-t1))
    print(time.localtime(time.time()))
