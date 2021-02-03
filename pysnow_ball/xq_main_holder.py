#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_holder import *

import time 
import datetime

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

token=get_cookie()
ball.set_token(token)

hdata_holder=HData_xq_holder("usr","usr")

#stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

def handle_raw_df(df):

    df=df.fillna(0)
   
    #timestamp -> date
    if 'timestamp' in df.columns:
        df['timestamp'] = df['timestamp'].apply(lambda x: get_date_from_timestamp(x))

    if 'report_date' in df.columns:
        df['report_date'] = df['report_date'].apply(lambda x: get_date_from_timestamp(x))

    if 'ctime' in df.columns:
        df['ctime'] = df['ctime'].apply(lambda x: get_date_from_timestamp(x))

    if 'report_date' in df.columns:
        df = df[df['report_date'] != '1970-01-01']
   

    new_cols = [ 'timestamp', 'symbol', 'per_float_chg','per_float', 'top_float_holder_ratio', \
            'chg', 'price', 'bshare_holder', 'ashare_holder', \
            'per_amount', 'top_holder_ratio', 'holder_num', 'hshare_holder']
    df=df[new_cols]

    return df
    

def get_original_data():

    df = pd.DataFrame()
    codestock_local=get_stock_list()
    length=len(codestock_local)
    tt_1 = time.time()
    t_1 = t_2 = 0
    mod = 1000
    for i in range(0,length):
        

        if i % mod == 0:
            t_1 = time.time()
        nowcode=codestock_local[i][0]
        if nowcode[0:1] == '6':
            stock_code_new= 'SH' + nowcode
        else:
            stock_code_new= 'SZ' + nowcode
        tmp_df = get_holder_data(stock_code_new, def_cnt=12)

        if debug:
            print(i, stock_code_new)
            print(tmp_df)
        #add stock_code
        #tmp_df['symbol'] = stock_code_new
        if len(tmp_df):
            tmp_df.insert(1, 'symbol' , stock_code_new, allow_duplicates=False)
            df = pd.concat([df, tmp_df])

        #debug
        if( 0 ):
            if i > 5:
                return df

        if i % (mod-1) == 0:
            t_2 = time.time()
            d_t = t_2 - t_1
            if debug:
                print(t_1, t_2)
                print('get_holder() i=%d, stock_code_new =%s ,d_t=%f, len(tmp_df)=%d' % \
                        (i, stock_code_new, d_t, len(tmp_df)))

    tt_2 = time.time()
    delta_t = tt_2 - tt_1
    if debug:
        print('get_holder() delta_t=%f' % delta_t)
        print('len(list(df))=%d' % len(list(df)))
        print('list(df)=%s' % list(df))

    return df

def get_holder():

    df = get_original_data()

    df = handle_raw_df(df)
    df = df.reset_index(drop=True)

    if debug:
        print('len(df)=%d' % len(df))
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


def update_database_holder():
    print('holder')
    df_holder = get_holder()
    df_holder.to_csv('./test_holder.csv', encoding='utf-8')
    if len(df_holder):
        hdata_holder.db_hdata_xq_create()
        hdata_holder.copy_from_stringio(df_holder)
    pass



if __name__ == '__main__':
    

    cript_name, para1 = check_input_parameter()
    
    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    update_database_holder()
    
    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
