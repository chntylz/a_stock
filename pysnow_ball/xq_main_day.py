#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_day import *
import  datetime

import time 

import pandas as pd
import numpy as np

import sys
sys.path.append("..")
import os

from test_get_basic_data import * 
from file_interface import * 
from comm_interface import * 


from Stocks import *

import pysnowball as ball

debug=0
#debug=1
para1 = 0

hdata_day=HData_xq_day("usr","usr")
stocks=Stocks("usr","usr")


#stocks.db_stocks_create()#如果还没有表则需要创建
stocks.db_stocks_update()#根据todayall的情况更新stocks表

#sys.exit()

def handle_raw_df(df):
    '''
    del df['volume_post']
    del df['amount_post']
    del df['hold_volume_hk']
    del df['hold_ratio_hk']
    del df['net_volume_hk']
    del df['balance']
    '''

    #add is_peach is_zig is_quad
    df['is_peach'] = 0
    df['is_zig'] = 0
    df['is_quad'] = 0
    df['is_macd'] = 0
    df['is_2d3pct'] = 0
    df['is_up_days'] = 0
    df['is_cup_tea'] = 0
    df['is_duck_head'] = 0


    df=df.fillna(0)
    
    df['market_capital'] = round(df['market_capital']/10000/10000, 2) # unit:y
    df['amount'] = round(df['amount']/10000/10000, 2) # unit:y
    df['hold_volume_cn'] = round(df['hold_volume_cn']/10000, 2) # unit:w
    df['volume'] = round(df['volume']/10000, 2) # unit:w


   
    #rename column
    df['hk_volume'] = df['hold_volume_cn']
    df['hk_pct'] = df['hold_ratio_cn']
    df['hk_net'] = df['net_volume_cn']

    del df['hold_volume_cn']
    del df['hold_ratio_cn']
    del df['net_volume_cn']

    new_cols = [ 'timestamp', 'symbol', 'open', 'close', 'high', 'low', 'volume', \
            'amount', 'percent', 'chg',\
            'turnoverrate', 'pe', 'pb', 'ps', 'pcf', 'market_capital', \
            'hk_volume', 'hk_pct', 'hk_net', \
            'is_peach', 'is_zig', 'is_quad', \
            'is_macd', 'is_2d3pct', 'is_up_days', 'is_cup_tea', 'is_duck_head']

    #resort conlums
    df = df[new_cols]

    #timestamp -> date
    df['timestamp'] = df['timestamp'].apply(lambda x: get_date_from_timestamp(x))
   
    return df
    

def get_today_data():
    
    #get A open date
    a_code = 'SH000001'
    a_df = get_his_data(a_code, 1)
    a_df['symbol'] = a_code
    a_df = handle_raw_df(a_df)
    cur_date = a_df['timestamp'][0]

    df = pd.DataFrame()
    codestock_local=get_stock_list()
    length=len(codestock_local)
    tt_1 = time.time()
    for i in range(0,length):
        nowcode=codestock_local[i][0]
        if nowcode[0:1] == '6':
            stock_code_new= 'SH' + nowcode
        else:
            stock_code_new= 'SZ' + nowcode
        tmp_df = get_his_data(stock_code_new, 1)
        #add stock_code
        tmp_df['symbol'] = stock_code_new
        df = pd.concat([df, tmp_df])

        if( 0 ):
            if i > 5:
                break

    tt_2 = time.time()
    delta_t = tt_2 - tt_1
    print('get_today_data() delta_t=%d' % delta_t)
    #delete data not today
    df = handle_raw_df(df)
    df = df[df['timestamp']==cur_date]
    df = df.reset_index(drop=True)

    if debug:
        print(df.head(10))

    return df


def get_all_his_data():
    codestock_local=get_stock_list()
    length=len(codestock_local)
    tt_1 = time.time()

    if int(para1):
        nowdate=datetime.datetime.now().date()
        nowdate=nowdate-datetime.timedelta(int(para1))

    for i in range(0,length):
        nowcode=codestock_local[i][0]
        if nowcode[0:1] == '6':
            stock_code_new= 'SH' + nowcode
        else:
            stock_code_new= 'SZ' + nowcode

        df = get_his_data(stock_code_new, 10000)
        #add stock_code
        df['symbol'] = stock_code_new
         
        df = handle_raw_df(df)
        
        if int(para1):
            #print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))
            df=df[df.timestamp == nowdate.strftime("%Y-%m-%d")]

        #insert database
        #todo
        hdata_day.copy_from_stringio(df)

        if( 0 ):
            if i > 5:
                break
    
    tt_2 = time.time()
    delta_t = tt_2 - tt_1
    print('get_all_his_data(): delta_t=%d' % delta_t)

    return df


def check_table():
    table_exist = hdata_day.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_day.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_day.db_hdata_xq_create()
        print('table not exist, create')



if __name__ == '__main__':

    token=get_cookie()
    ball.set_token(token)

        


    cript_name, para1 = check_input_parameter()
    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    #check table exist
    check_table()

    if int(para1):
        print('all data')
        get_all_his_data()
    else:
        print('today data')
        today_df = get_today_data()
        today_df.to_csv('./test_today.csv', encoding='gbk')
        hdata_day.delete_data_from_hdata(
                start_date=datetime.datetime.now().date().strftime("%Y-%m-%d"),
                end_date=datetime.datetime.now().date().strftime("%Y-%m-%d")
                )
        hdata_day.copy_from_stringio(today_df)
        #hdata_day.insert_all_stock_data_3(today_df)

    #delete closed stock data according amount=0
    #hdata_day.delete_amount_is_zero()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))


    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
