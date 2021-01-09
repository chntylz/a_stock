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

debug=0
#debug=1


hdata_day=HData_xq_day("usr","usr")

#stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

def handle_raw_df(df):
    del df['volume_post']
    del df['amount_post']
    del df['hold_volume_hk']
    del df['hold_ratio_hk']
    del df['net_volume_hk']
    del df['balance']

    #add is_peach is_zig is_quad
    df['is_peach'] = 0
    df['is_zig'] = 0
    df['is_quad'] = 0

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
            'is_quad', 'is_zig', 'is_quad']

    #resort conlums
    df = df[new_cols]

    #timestamp -> date
    df['timestamp'] = df['timestamp'].apply(lambda x: get_date_from_timestamp(x))
   
    return df
    

def get_today_data():
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
    df = df.reset_index(drop=True)
    df = handle_raw_df(df)

    if debug:
        print(df.head(10))

    return df


def get_all_his_data():
    codestock_local=get_stock_list()
    length=len(codestock_local)
    tt_1 = time.time()
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
        hdata_day.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_day.db_hdata_xq_create()
        print('table not exist, create')



if __name__ == '__main__':
    
    cript_name, para1 = check_input_parameter()
    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))


    check_table()
    if int(para1):
        get_all_his_data()
    else:
        today_df = get_today_data()
        hdata_day.copy_from_stringio(today_df)
        #hdata_day.insert_all_stock_data_3(today_df)

    #delete closed stock data according amount=0
    #hdata_day.delete_amount_is_zero()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))


    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
