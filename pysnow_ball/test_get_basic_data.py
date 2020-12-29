#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import pysnowball as ball
import datetime
from time import clock

import sys 
sys.path.append("..") 
from Stocks import *
stocks=Stocks("usr","usr")


import numpy as np
import pandas as pd


ball.set_token('xq_a_token=c833a63d6cd4b5033f1c789f2e08c2da787f32a3')

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

def get_stock_list():
    codestock_local=stocks.get_codestock_local()
    return codestock_local

def convert_data_list_to_df():
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

    t1 = clock()
    df = convert_data_list_to_df()
    t2 = clock()
    print("t1:%s, t2:%s, delta time=%s"%(t1, t2, t2-t1))

