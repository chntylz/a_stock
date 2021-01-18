#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_holder import *
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

import pysnowball as ball

debug=0
#debug=1


hdata_holder=HData_xq_holder("usr","usr")

#stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

def handle_raw_df(df):

    df=df.fillna(0)
   
   
    new_cols = [ 'timestamp', 'symbol', 'open', 'close', 'high', 'low', 'volume', \
            'amount', 'percent', 'chg',\
            'turnoverrate', 'pe', 'pb', 'ps', 'pcf', 'market_capital', \
            'hk_volume', 'hk_pct', 'hk_net', \
            'is_peach', 'is_zig', 'is_quad']


    new_cols = ['timestamp', 'symbol', 'per_float_chg', 'per_float', 'top_float_holder_ratio', 'chg', \
            'price', 'bshare_holder', 'ashare_holder', 'per_amount', 'top_holder_ratio', \
            'holder_num', 'hshare_holder']

    #resort conlums
    df = df[new_cols]

    #timestamp -> date
    df['timestamp'] = df['timestamp'].apply(lambda x: get_date_from_timestamp(x))
   
    return df
    

def get_holder():

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
        tmp_df = get_holder_data(stock_code_new, 20)
        #add stock_code
        tmp_df['symbol'] = stock_code_new
        df = pd.concat([df, tmp_df])

        #debug
        if( 0 ):
            if i > 5:
                break

    tt_2 = time.time()
    delta_t = tt_2 - tt_1
    print('get_holder() delta_t=%d' % delta_t)
    print(list(df))
    df = handle_raw_df(df)
    df = df.reset_index(drop=True)

    if debug:
        print(df.head(10))

    return df


def check_table():
    table_exist = hdata_holder.table_is_exist() 
    print('table_exist=%d' % table_exist)
    if table_exist:
        hdata_holder.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_holder.db_hdata_xq_create()
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
    else:
        print('today data')
        today_df = get_holder()
        hdata_holder.copy_from_stringio(today_df)
        #hdata_holder.insert_all_stock_data_3(today_df)

    #delete closed stock data according amount=0
    #hdata_holder.delete_amount_is_zero()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))


    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
